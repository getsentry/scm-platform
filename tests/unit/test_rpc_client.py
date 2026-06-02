from unittest.mock import MagicMock, patch

import msgspec
import pytest
from requests.exceptions import ChunkedEncodingError

from scm.errors import ErrorCode, SCMCodedError, TruncatedResponse
from scm.providers.github.provider import GitHubProvider
from scm.providers.gitlab.provider import GitLabProvider
from scm.rpc.client import (
    _DEFAULT_MAX_TRANSPORT_RETRIES,
    RpcApiClient,
    deserialize_repository,
    fetch_provider,
    fetch_repository,
)
from scm.rpc.server import serialize_repository
from scm.rpc.types import Error, ErrorResponse


def make_repository(**overrides):
    defaults = {
        "id": 1,
        "external_id": "abc123",
        "integration_id": 1,
        "is_active": True,
        "name": "org/repo",
        "organization_id": 1,
        "provider_name": "github",
        "web_base_url": None,
    }
    return {**defaults, **overrides}


def make_serialized_repository(**overrides):
    return serialize_repository(make_repository(**overrides))


def make_error_response(*codes: ErrorCode) -> bytes:
    return msgspec.json.encode(ErrorResponse(errors=[Error(code=code) for code in codes]))


class TestFetchRepository:
    @patch("scm.rpc.client.requests.get")
    def test_success(self, mock_get):
        mock_get.return_value = MagicMock(status_code=200, content=make_serialized_repository())
        repo = fetch_repository("http://base", "secret", 1, 1)
        assert repo["name"] == "org/repo"
        assert repo["provider_name"] == "github"

    @patch("scm.rpc.client.requests.get")
    def test_single_error_raises_coded_error(self, mock_get):
        mock_get.return_value = MagicMock(status_code=404, content=make_error_response("repository_not_found"))
        with pytest.raises(SCMCodedError, match="repository_not_found"):
            fetch_repository("http://base", "secret", 1, 1)

    @patch("scm.rpc.client.requests.get")
    def test_multiple_errors_raises_exception_group(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=400,
            content=make_error_response("repository_not_found", "rpc_invalid_grant"),
        )
        with pytest.raises(ExceptionGroup) as exc_info:
            fetch_repository("http://base", "secret", 1, 1)

        codes = {e.code for e in exc_info.value.exceptions}
        assert codes == {"repository_not_found", "rpc_invalid_grant"}

    @patch("scm.rpc.client.requests.get")
    def test_undeserializable_error_response(self, mock_get):
        mock_get.return_value = MagicMock(status_code=500, content=b"not valid json")
        with pytest.raises(SCMCodedError, match="rpc_errors_could_not_be_deserialized"):
            fetch_repository("http://base", "secret", 1, 1)

    @patch("scm.rpc.client.requests.get")
    def test_signs_get_request_headers(self, mock_get):
        mock_get.return_value = MagicMock(status_code=200, content=make_serialized_repository())
        fetch_repository("http://base", "secret", 1, 1)

        call_kwargs = mock_get.call_args
        headers = call_kwargs.kwargs["headers"]
        assert headers["Authorization"].startswith("rpcsignature rpc0:")
        assert headers["X-Organization-Id"] == "1"

    @patch("scm.rpc.client.requests.get")
    def test_tuple_repository_id(self, mock_get):
        mock_get.return_value = MagicMock(status_code=200, content=make_serialized_repository())
        fetch_repository("http://base", "secret", 1, ("github", "ext-123"))

        headers = mock_get.call_args.kwargs["headers"]
        assert headers["X-Repository-Id"] == '["github","ext-123"]'


class TestDeserializeRepository:
    def test_valid_content(self):
        repo = deserialize_repository(make_serialized_repository())
        assert repo["name"] == "org/repo"
        assert repo["external_id"] == "abc123"
        assert repo["integration_id"] == 1
        assert repo["is_active"] is True
        assert repo["organization_id"] == 1
        assert repo["provider_name"] == "github"

    def test_invalid_content_raises(self):
        with pytest.raises(SCMCodedError, match="repository_could_not_be_deserialized"):
            deserialize_repository(b"not valid json")

    def test_wrong_structure_raises(self):
        with pytest.raises(SCMCodedError, match="repository_could_not_be_deserialized"):
            deserialize_repository(b'{"type": "unknown", "data": {}}')


class TestFetchProvider:
    def test_github_returns_github_provider(self):
        client = MagicMock()
        repo = make_repository(provider_name="github")
        provider = fetch_provider(client, 1, repo)
        assert isinstance(provider, GitHubProvider)

    def test_github_enterprise_returns_github_provider(self):
        client = MagicMock()
        repo = make_repository(provider_name="github_enterprise", web_base_url="https://github.example.com")
        provider = fetch_provider(client, 1, repo)
        assert isinstance(provider, GitHubProvider)

    def test_github_enterprise_without_web_base_url_raises(self):
        client = MagicMock()
        repo = make_repository(provider_name="github_enterprise")
        with pytest.raises(SCMCodedError, match="rpc_invalid_grant"):
            fetch_provider(client, 1, repo)

    def test_gitlab_returns_gitlab_provider(self):
        client = MagicMock()
        repo = make_repository(provider_name="gitlab", external_id="gitlab.com:12345")
        provider = fetch_provider(client, 1, repo)
        assert isinstance(provider, GitLabProvider)

    def test_unknown_provider_returns_none(self):
        client = MagicMock()
        repo = make_repository(provider_name="bitbucket")
        provider = fetch_provider(client, 1, repo)
        assert provider is None


class TestRpcApiClient:
    def test_request_encodes_action_and_signs(self):
        client = RpcApiClient(
            full_url="http://base/api/0/internal/scm-rpc",
            signing_secret="secret",
            organization_id=1,
            referrer="test-referrer",
            repository_id=1,
        )
        mock_response = MagicMock()
        client.session = MagicMock()
        client.session.post.return_value = mock_response

        result = client.request(
            method="GET",
            path="/repos/org/repo/pulls/1",
            headers={"Accept": "application/json"},
            data=None,
            params={"per_page": "10"},
        )

        assert result is mock_response

        call_args = client.session.post.call_args
        assert call_args.kwargs["url"] == "http://base/api/0/internal/scm-rpc"

        headers = call_args.kwargs["headers"]
        assert headers["Authorization"].startswith("rpcsignature rpc0:")
        assert headers["Content-Type"] == "application/json"
        assert headers["X-Organization-Id"] == "1"
        assert headers["X-Referrer"] == "test-referrer"
        assert headers["X-Repository-Id"] == "1"

        body = call_args.kwargs["data"]
        decoded = msgspec.json.decode(body)
        assert decoded["type"] == "action"
        assert decoded["data"]["method"] == "GET"
        assert decoded["data"]["path"] == "/repos/org/repo/pulls/1"
        assert decoded["data"]["headers"] == {"Accept": "application/json"}
        assert decoded["data"]["params"] == {"per_page": "10"}

    def test_request_with_tuple_repository_id(self):
        client = RpcApiClient(
            full_url="http://base/api/0/internal/scm-rpc",
            signing_secret="secret",
            organization_id=1,
            referrer="shared",
            repository_id=("github", "ext-123"),
        )
        client.session = MagicMock()
        client.session.post.return_value = MagicMock()

        client.request(method="GET", path="/test")

        headers = client.session.post.call_args.kwargs["headers"]
        assert headers["X-Repository-Id"] == '["github","ext-123"]'


class TestRpcApiClientTransportRetry:
    """The proxy commits a 200 before streaming the upstream body, so a mid-stream disconnect
    surfaces here as a transport abort. Idempotent reads retry; everything else fails fast as a
    typed, retriable ``TruncatedResponse``."""

    def _make_client(self, max_transport_retries: int = _DEFAULT_MAX_TRANSPORT_RETRIES) -> RpcApiClient:
        client = RpcApiClient(
            full_url="http://base/api/0/internal/scm-rpc",
            signing_secret="secret",
            organization_id=1,
            referrer="shared",
            repository_id=1,
            max_transport_retries=max_transport_retries,
        )
        client.session = MagicMock()
        return client

    @patch("scm.rpc.client.time.sleep")
    def test_retries_idempotent_get_then_succeeds(self, mock_sleep):
        client = self._make_client()
        ok = MagicMock()
        client.session.post.side_effect = [ChunkedEncodingError("Response ended prematurely"), ok]

        result = client.request(method="GET", path="/repos/org/repo/git/trees/abc")

        assert result is ok
        assert client.session.post.call_count == 2
        mock_sleep.assert_called_once()

    @patch("scm.rpc.client.time.sleep")
    def test_raises_truncated_response_after_exhausting_retries(self, mock_sleep):
        client = self._make_client(max_transport_retries=2)
        client.session.post.side_effect = ChunkedEncodingError("Response ended prematurely")

        with pytest.raises(TruncatedResponse) as exc_info:
            client.request(method="GET", path="/repos/org/repo/git/trees/abc")

        assert exc_info.value.retriable is True
        # initial attempt + 2 retries
        assert client.session.post.call_count == 3

    @patch("scm.rpc.client.time.sleep")
    def test_does_not_retry_non_idempotent_method(self, mock_sleep):
        client = self._make_client()
        client.session.post.side_effect = ChunkedEncodingError("Response ended prematurely")

        with pytest.raises(TruncatedResponse):
            client.request(method="POST", path="/repos/org/repo/pulls/1/reviews", data={"body": "x"})

        assert client.session.post.call_count == 1
        mock_sleep.assert_not_called()
