from unittest.mock import MagicMock

import msgspec
import pytest

from scm.errors import SCMCodedError
from scm.rpc.helpers import deserialize_repository, sign_get, sign_post
from scm.rpc.server import RpcServer, is_safe_path, iter_response, normalize_headers, serialize_repository
from scm.test_fixtures import BaseTestProvider
from scm.types import Repository

TEST_SECRET = "test-secret"


def make_repository(**overrides) -> Repository:
    defaults: Repository = {
        "id": 1,
        "external_id": "abc123",
        "integration_id": 1,
        "is_active": True,
        "name": "org/repo",
        "organization_id": 1,
        "provider_name": "github",
    }
    return {**defaults, **overrides}  # type: ignore[typeddict-item]


def mock_record_count(a, b, c):
    return None


def make_server(**overrides) -> RpcServer:
    defaults = dict(
        secrets=[TEST_SECRET],
        fetch_repository=lambda org_id, repo_id: make_repository(),
        fetch_provider=lambda org_id, repo: BaseTestProvider(),
        record_count=mock_record_count,
        emit_error=lambda e: None,
    )
    defaults.update(overrides)
    return RpcServer(**defaults)  # type: ignore[arg-type]


def make_headers(**overrides) -> dict[str, str]:
    defaults = {
        "Authorization": "rpcsignature rpc0:abc",
        "X-Organization-Id": "1",
        "X-Repository-Id": "1",
    }
    defaults.update(overrides)
    return defaults


class TestExtractHeaders:
    def test_extracts_int_repository_id(self):
        server = make_server()
        auth, org_id, repo_id = server._extract_headers(make_headers())
        assert auth == "rpc0:abc"
        assert org_id == 1
        assert repo_id == 1

    def test_extracts_tuple_repository_id(self):
        server = make_server()
        headers = make_headers(**{"X-Repository-Id": '["github","ext-123"]'})
        _, _, repo_id = server._extract_headers(headers)
        assert repo_id == ("github", "ext-123")

    def test_missing_authorization_raises(self):
        server = make_server()
        headers = make_headers()
        del headers["Authorization"]
        with pytest.raises(SCMCodedError, match="rpc_malformed_request_headers"):
            server._extract_headers(headers)

    def test_missing_organization_id_raises(self):
        server = make_server()
        headers = make_headers()
        del headers["X-Organization-Id"]
        with pytest.raises(SCMCodedError, match="rpc_malformed_request_headers"):
            server._extract_headers(headers)

    def test_non_integer_organization_id_raises(self):
        server = make_server()
        headers = make_headers(**{"X-Organization-Id": "not-a-number"})
        with pytest.raises(SCMCodedError, match="rpc_malformed_request_headers"):
            server._extract_headers(headers)

    def test_invalid_repository_id_json_raises(self):
        server = make_server()
        headers = make_headers(**{"X-Repository-Id": "not-json{"})
        with pytest.raises(SCMCodedError, match="rpc_malformed_request_headers"):
            server._extract_headers(headers)


class TestGet:
    def test_returns_serialized_repository(self):
        repo = make_repository()
        provider = BaseTestProvider()
        provider.repository = repo
        server = make_server(
            fetch_repository=lambda org_id, repo_id: repo,
            fetch_provider=lambda org_id, r: provider,
        )

        response = server.get(make_headers(Authorization=sign_get(TEST_SECRET, 1, 1)))

        assert response.status_code == 200
        decoded = deserialize_repository(response.content)
        assert decoded["name"] == "org/repo"
        assert decoded["provider_name"] == "github"

    def test_invalid_signature_returns_401(self):
        server = make_server()
        response = server.get(make_headers())

        assert response.status_code == 401
        decoded = msgspec.json.decode(response.content)
        assert decoded["errors"][0]["code"] == "rpc_invalid_grant"

    def test_repository_not_found_returns_404(self):
        server = make_server(fetch_repository=lambda org_id, repo_id: None)
        response = server.get(make_headers(Authorization=sign_get(TEST_SECRET, 1, 1)))

        assert response.status_code == 404
        decoded = msgspec.json.decode(response.content)
        assert decoded["errors"][0]["code"] == "repository_not_found"

    def test_malformed_headers_returns_400(self):
        server = make_server()
        response = server.get({"Authorization": "x"})

        assert response.status_code == 400
        decoded = msgspec.json.decode(response.content)
        assert decoded["errors"][0]["code"] == "rpc_malformed_request_headers"


class TestPost:
    def _make_action_body(self, **data_overrides) -> bytes:
        data = {
            "method": "GET",
            "path": "/repos/org/repo/branches/main",
            "headers": None,
            "data": None,
            "params": None,
            "allow_redirects": True,
            "stream": None,
            **data_overrides,
        }
        return msgspec.json.encode({"type": "action", "data": data})

    def test_invalid_signature_returns_401(self):
        server = make_server()
        response = server.post(self._make_action_body(), make_headers())

        assert response.status_code == 401
        decoded = msgspec.json.decode(b"".join(response.content))
        assert decoded["errors"][0]["code"] == "rpc_invalid_grant"

    def test_malformed_body_returns_400(self):
        body = b"not valid json"
        server = make_server()
        response = server.post(body, make_headers(Authorization=sign_post(TEST_SECRET, body)))

        assert response.status_code == 400
        decoded = msgspec.json.decode(b"".join(response.content))
        assert decoded["errors"][0]["code"] == "rpc_malformed_request_body"

    def test_malformed_headers_returns_400(self):
        server = make_server()
        response = server.post(self._make_action_body(), {"Authorization": "x"})

        assert response.status_code == 400
        decoded = msgspec.json.decode(b"".join(response.content))
        assert decoded["errors"][0]["code"] == "rpc_malformed_request_headers"

    def test_repository_not_found_returns_404(self):
        body = self._make_action_body()
        server = make_server(fetch_repository=lambda org_id, repo_id: None)
        response = server.post(body, make_headers(Authorization=sign_post(TEST_SECRET, body)))

        assert response.status_code == 404
        decoded = msgspec.json.decode(b"".join(response.content))
        assert decoded["errors"][0]["code"] == "repository_not_found"

    def test_request_at_10mb_returns_413(self):
        body = b"x" * (10 * 1024 * 1024)
        server = make_server()
        response = server.post(body, make_headers(Authorization=sign_post(TEST_SECRET, body)))

        assert response.status_code == 413
        decoded = msgspec.json.decode(b"".join(response.content))
        assert decoded["errors"][0]["code"] == "rpc_request_too_large"

    def test_request_just_under_10mb_is_not_rejected_for_size(self):
        body = self._make_action_body(data="a" * (10 * 1024 * 1024 - 200))
        server = make_server()
        response = server.post(body, make_headers(Authorization=sign_post(TEST_SECRET, body)))

        assert response.status_code != 413

    def test_unsafe_path_returns_400(self):
        body = self._make_action_body(path="https://evil.com/repos")
        server = make_server()
        response = server.post(body, make_headers(Authorization=sign_post(TEST_SECRET, body)))

        assert response.status_code == 400
        decoded = msgspec.json.decode(b"".join(response.content))
        assert decoded["errors"][0]["code"] == "rpc_invalid_path"

    def test_scheme_relative_path_returns_400(self):
        body = self._make_action_body(path="//evil.com/repos")
        server = make_server()
        response = server.post(body, make_headers(Authorization=sign_post(TEST_SECRET, body)))

        assert response.status_code == 400
        decoded = msgspec.json.decode(b"".join(response.content))
        assert decoded["errors"][0]["code"] == "rpc_invalid_path"

    def test_action_headers_are_truncated_to_allowlist(self):
        repo = make_repository()
        provider = MagicMock()
        provider.repository = repo
        provider.is_rate_limited.return_value = False
        provider.__class__.__name__ = "GitHubProvider"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.iter_content.return_value = [b"ok"]
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        provider.request.return_value = mock_response

        server = make_server(
            fetch_repository=lambda org_id, repo_id: repo,
            fetch_provider=lambda org_id, r: provider,
        )

        body = self._make_action_body(
            headers={
                "Accept": "application/json",
                "Content-Type": "text/plain",
                "If-None-Match": '"etag"',
                "Authorization": "Bearer stolen",
                "X-Custom": "dropped",
            },
        )
        response = server.post(body, make_headers(Authorization=sign_post(TEST_SECRET, body)))

        assert response.status_code == 200
        forwarded_headers = provider.request.call_args.kwargs["headers"]
        assert forwarded_headers == {
            "Accept": "application/json",
            "Content-Type": "text/plain",
            "If-None-Match": '"etag"',
        }

    def test_successful_post_streams_response(self):
        repo = make_repository()
        provider = MagicMock()
        provider.repository = repo
        provider.is_rate_limited.return_value = False
        provider.__class__.__name__ = "GitHubProvider"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.iter_content.return_value = [b'{"ref": "main"}']
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        provider.request.return_value = mock_response

        server = make_server(
            fetch_repository=lambda org_id, repo_id: repo,
            fetch_provider=lambda org_id, r: provider,
        )

        body = self._make_action_body()
        response = server.post(body, make_headers(Authorization=sign_post(TEST_SECRET, body)))

        assert response.status_code == 200
        assert response.headers["Content-Type"] == "application/json"
        chunks = list(response.content)
        assert b'{"ref": "main"}' in chunks


class TestSerializeRepository:
    def test_serializes_all_fields(self):
        repo = make_repository()
        decoded = deserialize_repository(serialize_repository(repo))

        assert decoded["external_id"] == repo["external_id"]
        assert decoded["integration_id"] == repo["integration_id"]
        assert decoded["is_active"] == repo["is_active"]
        assert decoded["name"] == repo["name"]
        assert decoded["organization_id"] == repo["organization_id"]
        assert decoded["provider_name"] == repo["provider_name"]

    def test_null_external_id(self):
        assert deserialize_repository(serialize_repository(make_repository(external_id=None)))["external_id"] is None


class TestIterResponse:
    def test_yields_chunks(self):
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b"chunk1", b"chunk2", b""]
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        chunks = list(iter_response(mock_response))
        assert chunks == [b"chunk1", b"chunk2"]

    def test_skips_empty_chunks(self):
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b"", b"data", b"", b"more"]
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        chunks = list(iter_response(mock_response))
        assert chunks == [b"data", b"more"]


class TestNormalizeHeaders:
    def test_passes_through_regular_headers(self):
        headers = {"Content-Type": "application/json", "X-Custom": "value"}
        result = normalize_headers(headers)
        assert result == {"Content-Type": "application/json", "X-Custom": "value"}

    def test_strips_hop_by_hop_headers(self):
        headers = {
            "Content-Type": "application/json",
            "transfer-encoding": "chunked",
            "connection": "keep-alive",
            "content-encoding": "gzip",
            "content-length": "42",
        }
        result = normalize_headers(headers)
        assert result == {"Content-Type": "application/json"}

    def test_strip_is_case_insensitive(self):
        headers = {"Transfer-Encoding": "chunked", "CONNECTION": "keep-alive"}
        result = normalize_headers(headers)
        assert result == {}

    def test_empty_headers(self):
        assert normalize_headers({}) == {}

    def test_strips_auth_and_cookie_headers(self):
        headers = {
            "authorization": "Bearer token",
            "set-cookie": "session=abc",
            "proxy-authenticate": "Basic",
            "proxy-authorization": "Basic xyz",
            "X-Request-Id": "123",
        }
        result = normalize_headers(headers)
        assert result == {"X-Request-Id": "123"}


class TestIsSafePath:
    def test_absolute_path(self):
        assert is_safe_path("/repos/org/repo") is True

    def test_path_with_query_string(self):
        assert is_safe_path("/repos/org/repo?page=1") is True

    def test_rejects_relative_path(self):
        assert is_safe_path("repos/org/repo") is False

    def test_rejects_empty_string(self):
        assert is_safe_path("") is False

    def test_rejects_absolute_url_with_scheme(self):
        assert is_safe_path("https://evil.com/repos") is False

    def test_rejects_scheme_relative_url(self):
        assert is_safe_path("//evil.com/repos") is False

    def test_rejects_scheme_with_authority(self):
        assert is_safe_path("http://evil.com/repos") is False

    def test_root_path(self):
        assert is_safe_path("/") is True

    def test_path_with_encoded_characters(self):
        assert is_safe_path("/repos/org/repo%20name") is True
