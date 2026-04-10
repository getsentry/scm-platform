import json
from io import BytesIO
from unittest.mock import MagicMock

import msgspec
import requests

from scm.actions import create_issue_comment, get_pull_request
from scm.rpc.client import SourceCodeManager, deserialize_repository
from scm.rpc.errors import deserialize_error
from scm.rpc.helpers import sign_get
from scm.rpc.server import RpcServer
from scm.types import Repository
from tests.test_fixtures import make_github_comment, make_github_pull_request

SIGNING_SECRET = "test-secret"
BASE_URL = "http://rpc-server"


def make_repository(**overrides) -> Repository:
    defaults: Repository = {
        "external_id": "abc123",
        "integration_id": 1,
        "is_active": True,
        "name": "org/repo",
        "organization_id": 1,
        "provider_name": "github",
    }
    return {**defaults, **overrides}


def make_github_api_response(body: dict | list, status_code: int = 200) -> MagicMock:
    """A mock requests.Response as returned by the GitHub API."""
    content = json.dumps(body).encode()
    response = MagicMock(spec=requests.Response)
    response.status_code = status_code
    response.headers = {"Content-Type": "application/json"}
    response.iter_content.return_value = [content]
    response.__enter__ = lambda s: s
    response.__exit__ = MagicMock(return_value=False)
    return response


def make_rpc_server(repository: Repository, server_provider: MagicMock) -> RpcServer:
    return RpcServer(
        fetch_repository=lambda org_id, repo_id: repository,
        fetch_provider=lambda org_id, repo: server_provider,
        record_count=lambda name, value, tags: None,
        verify_request_signature=lambda auth, data: True,
    )


def bridged_fetch_repository(server: RpcServer):
    """Return a fetch_repository callable that routes through the RPC server."""

    def _fetch(base_url, signing_secret, org_id, repo_id):
        headers = {
            "Authorization": f"rpcsignature {sign_get(signing_secret, org_id, repo_id)}",
            "X-Organization-Id": str(org_id),
            "X-Repository-Id": msgspec.json.encode(repo_id).decode("utf-8"),
        }
        response = server.get(headers)
        if response.status_code != 200:
            deserialize_error(response.content)
        return deserialize_repository(response.content)

    return _fetch


def bridge_session_to_server(session: requests.Session, server: RpcServer) -> None:
    """Patch a requests.Session so POST calls route directly to the RPC server."""

    def fake_post(url, **kwargs):
        response = server.post(kwargs.get("data", b""), kwargs.get("headers", {}))
        raw = BytesIO(b"".join(response.content))
        mock_resp = requests.Response()
        mock_resp.status_code = response.status_code
        mock_resp.headers.update(response.headers)
        mock_resp.raw = raw
        mock_resp._content = raw.getvalue()
        return mock_resp

    session.post = fake_post


def make_client_scm(organization_id, repository_id, server):
    """Build an RPC client SourceCodeManager wired to the given RPC server."""
    scm = SourceCodeManager.make_from_repository_id(
        organization_id,
        repository_id,
        fetch_repository=bridged_fetch_repository(server),
        fetch_base_url=lambda: BASE_URL,
        fetch_signing_secret=lambda: SIGNING_SECRET,
    )
    bridge_session_to_server(scm.provider.client.session, server)
    return scm


class TestRpcIntegration:
    def test_get_pull_request(self):
        repo = make_repository()
        pr_json = make_github_pull_request()

        server_provider = MagicMock()
        server_provider.repository = repo
        server_provider.is_rate_limited.return_value = False
        server_provider.__class__.__name__ = "GitHubProvider"
        server_provider._request.return_value = make_github_api_response(pr_json)

        server = make_rpc_server(repo, server_provider)
        scm = make_client_scm(1, 1, server)

        result = get_pull_request(scm, "1")

        assert result["data"]["title"] == "Test PR"
        assert result["data"]["number"] == "1"
        assert result["data"]["state"] == "open"
        assert result["type"] == "github"

        call_kwargs = server_provider._request.call_args.kwargs
        assert call_kwargs["method"] == "GET"
        assert "/pulls/1" in call_kwargs["path"]

    def test_create_issue_comment(self):
        repo = make_repository()
        comment_json = make_github_comment(body="Hello from RPC")

        server_provider = MagicMock()
        server_provider.repository = repo
        server_provider.is_rate_limited.return_value = False
        server_provider.__class__.__name__ = "GitHubProvider"
        server_provider._request.return_value = make_github_api_response(comment_json, status_code=201)

        server = make_rpc_server(repo, server_provider)
        scm = make_client_scm(1, 1, server)

        result = create_issue_comment(scm, issue_id="10", body="Hello from RPC")

        assert result["data"]["body"] == "Hello from RPC"
        assert result["type"] == "github"

        call_kwargs = server_provider._request.call_args.kwargs
        assert call_kwargs["method"] == "POST"
        assert "/issues/10/comments" in call_kwargs["path"]
        assert call_kwargs["data"] == {"body": "Hello from RPC"}
