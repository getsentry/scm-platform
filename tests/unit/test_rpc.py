import hashlib
import hmac
from unittest.mock import MagicMock, patch

import msgspec
import pytest

from scm.actions import get_branch
from scm.errors import SCMError, SCMRepositoryCouldNotBeDeserialized, SCMRpcError
from scm.providers.github.provider import GitHubProvider
from scm.providers.gitlab.provider import GitLabProvider
from scm.rpc.client import (
    SCM_API_URL,
    NoOpRateLimitProvider,
    RpcApiClient,
    SourceCodeManager,
    deserialize_repository,
    fetch_repository,
    initialize_provider,
    raise_rpc_errors,
    sign_message,
)
from scm.types import Repository


def _make_repository(provider_name: str = "github") -> Repository:
    return Repository(
        external_id="12345",
        integration_id=1,
        is_active=True,
        name="org/repo",
        organization_id=1,
        provider_name=provider_name,
    )


class TestNoOpRateLimitProvider:
    def test_get_and_set_rate_limit(self):
        provider = NoOpRateLimitProvider()
        result = provider.get_and_set_rate_limit("total", "usage", 60)
        assert result == (None, 0)

    def test_get_accounted_usage(self):
        provider = NoOpRateLimitProvider()
        assert provider.get_accounted_usage(["key1", "key2"]) == 0

    def test_set_key_values(self):
        provider = NoOpRateLimitProvider()
        assert provider.set_key_values({"k": (1, None)}) is None


class TestSignMessage:
    def test_produces_valid_hmac(self):
        secret = "my-secret"
        message = b"hello world"
        result = sign_message(secret, message)

        expected_hmac = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()
        assert result == f"rpc0:{expected_hmac}"

    def test_different_secrets_produce_different_signatures(self):
        message = b"same message"
        sig1 = sign_message("secret-a", message)
        sig2 = sign_message("secret-b", message)
        assert sig1 != sig2

    def test_different_messages_produce_different_signatures(self):
        secret = "same-secret"
        sig1 = sign_message(secret, b"message-a")
        sig2 = sign_message(secret, b"message-b")
        assert sig1 != sig2


class TestRpcApiClient:
    def _make_client(self, **kwargs):
        defaults = dict(
            base_url="https://sentry.io",
            signing_secret="secret",
            organization_id=1,
            referrer="test",
            repository_id=42,
        )
        defaults.update(kwargs)
        return RpcApiClient(**defaults)

    def test_request_sends_post_to_scm_url(self):
        client = self._make_client()
        mock_response = MagicMock()
        client.session = MagicMock()
        client.session.post.return_value = mock_response

        response = client._request(
            method="GET",
            path="/repos/org/repo",
            headers={"Accept": "application/json"},
            data=None,
            params={"page": "1"},
            allow_redirects=True,
            raw_response=False,
        )

        assert response is mock_response
        client.session.post.assert_called_once()

        call_args = client.session.post.call_args
        assert call_args[0][0] == SCM_API_URL.format(base_url="https://sentry.io")

    def test_request_body_contains_proxied_request_fields(self):
        client = self._make_client()
        client.session = MagicMock()
        client.session.post.return_value = MagicMock()

        client._request(
            method="POST",
            path="/repos/org/repo/issues",
            headers={"X-Custom": "value"},
            data={"title": "bug"},
            params=None,
            allow_redirects=False,
            raw_response=False,
        )

        call_kwargs = client.session.post.call_args
        body = msgspec.json.decode(call_kwargs[1]["data"])
        assert body["method"] == "POST"
        assert body["path"] == "/repos/org/repo/issues"
        assert body["headers"] == {"X-Custom": "value"}
        assert body["data"] == {"title": "bug"}
        assert body["params"] is None
        assert body["allow_redirects"] is False

    def test_request_headers_include_auth_and_metadata(self):
        client = self._make_client(
            signing_secret="test-secret",
            organization_id=99,
            referrer="my-referrer",
            repository_id=42,
        )
        client.session = MagicMock()
        client.session.post.return_value = MagicMock()

        client._request(
            method="GET",
            path="/test",
            headers=None,
            data=None,
            params=None,
            allow_redirects=None,
            raw_response=False,
        )

        call_kwargs = client.session.post.call_args
        headers = call_kwargs[1]["headers"]

        assert headers["Content-Type"] == "application/json"
        assert headers["X-Organization-Id"] == "99"
        assert headers["X-Referrer"] == "my-referrer"
        assert headers["X-Repository-Id"] == "42"
        assert headers["Authorization"].startswith("rpcsignature rpc0:")

    def test_request_signs_body(self):
        client = self._make_client(signing_secret="the-secret")
        client.session = MagicMock()
        client.session.post.return_value = MagicMock()

        client._request(
            method="GET",
            path="/test",
            headers=None,
            data=None,
            params=None,
            allow_redirects=None,
            raw_response=False,
        )

        call_kwargs = client.session.post.call_args
        sent_body = call_kwargs[1]["data"]
        sent_auth = call_kwargs[1]["headers"]["Authorization"]

        expected_sig = sign_message("the-secret", sent_body)
        assert sent_auth == f"rpcsignature {expected_sig}"

    def test_repository_id_tuple_serialized(self):
        client = self._make_client(repository_id=("github", "ext-123"))
        client.session = MagicMock()
        client.session.post.return_value = MagicMock()

        client._request(
            method="GET",
            path="/test",
            headers=None,
            data=None,
            params=None,
            allow_redirects=None,
            raw_response=False,
        )

        call_kwargs = client.session.post.call_args
        headers = call_kwargs[1]["headers"]
        repo_id = headers["X-Repository-Id"]
        assert repo_id == '["github","ext-123"]'


class TestInitializeProvider:
    def test_github_returns_github_provider(self):
        client = MagicMock()
        repo = _make_repository("github")
        provider = initialize_provider(client, 1, repo)
        assert isinstance(provider, GitHubProvider)

    def test_gitlab_returns_gitlab_provider(self):
        client = MagicMock()
        repo = _make_repository("gitlab")
        repo["external_id"] = "gitlab.com:12345"
        provider = initialize_provider(client, 1, repo)
        assert isinstance(provider, GitLabProvider)

    def test_unsupported_provider_raises(self):
        client = MagicMock()
        repo = _make_repository("bitbucket")
        with pytest.raises(NotImplementedError):
            initialize_provider(client, 1, repo)


class TestSourceCodeManagerRpc:
    """Integration test: SourceCodeManager + actions route through the RPC URL."""

    def _make_scm(self, mock_session):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.json.return_value = {
            "name": "main",
            "commit": {"sha": "abc123"},
        }
        mock_session.post.return_value = mock_response

        repo = _make_repository("github")

        def fetch_repository(base_url, signing_secret, organization_id, repository_id):
            return repo

        scm = SourceCodeManager.make_from_repository_id(
            organization_id=1,
            repository_id=42,
            base_url="https://sentry.io",
            referrer="test",
            signing_secret="test-secret",
            fetch_repository=fetch_repository,
        )

        # Swap the real session with our mock.
        assert isinstance(scm.provider, GitHubProvider)
        assert isinstance(scm.provider.client.client, RpcApiClient)
        scm.provider.client.client.session = mock_session
        return scm

    def test_get_branch_routes_through_rpc(self):
        mock_session = MagicMock()
        scm = self._make_scm(mock_session)

        result = get_branch(scm, "main")

        mock_session.post.assert_called_once()
        call_args = mock_session.post.call_args

        assert call_args[0][0] == "https://sentry.io/api/0/internal/scm-rpc"

        body = msgspec.json.decode(call_args[1]["data"])
        assert body["method"] == "GET"
        assert body["path"] == "/repos/org/repo/branches/main"

        headers = call_args[1]["headers"]
        assert headers["Authorization"].startswith("rpcsignature rpc0:")
        assert headers["X-Organization-Id"] == "1"
        assert headers["X-Referrer"] == "test"

        assert result["data"]["ref"] == "main"
        assert result["data"]["sha"] == "abc123"


class TestSourceCodeManager:
    def test_make_from_repository_id(self):
        mock_repo = _make_repository("github")
        mock_fetch = MagicMock(return_value=mock_repo)

        with (
            patch("scm.rpc.client.RpcApiClient") as MockClient,
            patch("scm.rpc.client.initialize_provider") as mock_init,
        ):
            mock_provider = MagicMock()
            mock_init.return_value = mock_provider

            scm = SourceCodeManager.make_from_repository_id(
                organization_id=1,
                repository_id=42,
                base_url="https://sentry.io",
                referrer="test",
                signing_secret="secret",
                fetch_repository=mock_fetch,
            )

            mock_fetch.assert_called_once_with("https://sentry.io", "secret", 1, 42)
            MockClient.assert_called_once_with(
                base_url="https://sentry.io",
                signing_secret="secret",
                organization_id=1,
                referrer="test",
                repository_id=42,
            )
            mock_init.assert_called_once_with(MockClient.return_value, 1, mock_repo)

            assert scm.provider is mock_provider
            assert scm.referrer == "test"
            assert scm.organization_id == 1
            assert scm.repository_id == 42


class TestDeserializeRepository:
    def test_valid_response(self):
        data = msgspec.json.encode(
            {
                "external_id": "12345",
                "integration_id": 1,
                "is_active": True,
                "name": "org/repo",
                "organization_id": 1,
                "provider_name": "github",
            }
        )
        result = deserialize_repository(data)
        assert result == {
            "external_id": "12345",
            "integration_id": 1,
            "is_active": True,
            "name": "org/repo",
            "organization_id": 1,
            "provider_name": "github",
        }

    def test_null_external_id(self):
        data = msgspec.json.encode(
            {
                "external_id": None,
                "integration_id": 1,
                "is_active": True,
                "name": "org/repo",
                "organization_id": 1,
                "provider_name": "gitlab",
            }
        )
        result = deserialize_repository(data)
        assert result["external_id"] is None

    def test_invalid_json_raises(self):
        with pytest.raises(SCMRepositoryCouldNotBeDeserialized):
            deserialize_repository(b"not json")

    def test_missing_field_raises(self):
        data = msgspec.json.encode({"external_id": "12345"})
        with pytest.raises(SCMRepositoryCouldNotBeDeserialized):
            deserialize_repository(data)


class TestRaiseRpcErrors:
    def test_single_error(self):
        data = msgspec.json.encode(
            {"errors": [{"status": "400", "code": "bad_request", "title": "Bad", "detail": "Oops", "meta": None}]}
        )
        with pytest.raises(SCMRpcError) as exc_info:
            raise_rpc_errors(data)
        assert exc_info.value.code == "bad_request"
        assert exc_info.value.detail == "Oops"
        assert exc_info.value.status == "400"

    def test_multiple_errors_raises_exception_group(self):
        data = msgspec.json.encode(
            {
                "errors": [
                    {"status": "400", "code": "err1", "title": "First", "detail": None, "meta": None},
                    {"status": "500", "code": "err2", "title": "Second", "detail": None, "meta": None},
                ]
            }
        )
        with pytest.raises(ExceptionGroup) as exc_info:
            raise_rpc_errors(data)
        assert len(exc_info.value.exceptions) == 2

    def test_unparseable_error_response(self):
        with pytest.raises(SCMError, match="Unprocessable entity"):
            raise_rpc_errors(b"not json")

    def test_error_with_meta(self):
        data = msgspec.json.encode(
            {
                "errors": [
                    {
                        "status": "429",
                        "code": "rate_limited",
                        "title": None,
                        "detail": None,
                        "meta": {"retry_after": 30},
                    }
                ]
            }
        )
        with pytest.raises(SCMRpcError) as exc_info:
            raise_rpc_errors(data)

        assert exc_info.value.code == "rate_limited"
        assert exc_info.value.status == "429"
        assert exc_info.value.detail is None
        assert exc_info.value.title is None
        assert exc_info.value.meta == {"retry_after": 30}


class TestFetchRepository:
    def test_success(self):
        repo_data = {
            "external_id": "12345",
            "integration_id": 1,
            "is_active": True,
            "name": "org/repo",
            "organization_id": 1,
            "provider_name": "github",
        }
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = msgspec.json.encode(repo_data)

        with patch("requests.get", return_value=mock_response) as mock_get:
            result = fetch_repository("https://sentry.io", "secret", 1, 42)

        assert result["name"] == "org/repo"
        assert result["provider_name"] == "github"

        call_args = mock_get.call_args
        assert call_args[0][0] == "https://sentry.io/api/0/internal/scm-rpc"
        headers = call_args[1]["headers"]
        assert headers["Authorization"].startswith("rpcsignature rpc0:")
        assert headers["X-Organization-Id"] == "1"
        assert headers["X-Repository-Id"] == "42"

    def test_success_with_tuple_repository_id(self):
        repo_data = {
            "external_id": "12345",
            "integration_id": 1,
            "is_active": True,
            "name": "org/repo",
            "organization_id": 1,
            "provider_name": "github",
        }
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = msgspec.json.encode(repo_data)

        with patch("requests.get", return_value=mock_response) as mock_get:
            fetch_repository("https://sentry.io", "secret", 1, ("github", "ext-123"))

        headers = mock_get.call_args[1]["headers"]
        assert headers["X-Repository-Id"] == '["github","ext-123"]'

    def test_signs_url_not_body(self):
        repo_data = {
            "external_id": "12345",
            "integration_id": 1,
            "is_active": True,
            "name": "org/repo",
            "organization_id": 1,
            "provider_name": "github",
        }
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = msgspec.json.encode(repo_data)

        with patch("requests.get", return_value=mock_response) as mock_get:
            fetch_repository("https://sentry.io", "my-secret", 1, 42)

        headers = mock_get.call_args[1]["headers"]
        url = "https://sentry.io/api/0/internal/scm-rpc"
        expected_sig = sign_message("my-secret", url.encode())
        assert headers["Authorization"] == f"rpcsignature {expected_sig}"

    def test_error_response_raises(self):
        error_data = {
            "errors": [{"status": "404", "code": "not_found", "title": "Not Found", "detail": None, "meta": None}]
        }
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.content = msgspec.json.encode(error_data)

        with patch("requests.get", return_value=mock_response):
            with pytest.raises(SCMRpcError) as exc_info:
                fetch_repository("https://sentry.io", "secret", 1, 42)
            assert exc_info.value.code == "not_found"
