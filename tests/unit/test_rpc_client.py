from unittest.mock import MagicMock, patch

import msgspec
import pytest

from scm.errors import SCMCodedError
from scm.rpc.client import (
    NoOpRateLimitProvider,
    RpcApiClient,
    deserialize_repository,
    fetch_provider,
    fetch_repository,
)
from scm.rpc.types import RepositoryAttributes, RepositoryResponse
from scm.types import Repository


def make_repository(**overrides) -> Repository:
    defaults = {
        "external_id": "abc",
        "integration_id": 1,
        "is_active": True,
        "name": "org/repo",
        "organization_id": 1,
        "provider_name": "github",
    }
    defaults.update(overrides)
    return defaults


def make_repository_response(**overrides) -> bytes:
    defaults = dict(
        external_id="abc123",
        integration_id=1,
        is_active=True,
        name="org/repo",
        organization_id=1,
        provider_name="github",
    )
    defaults.update(overrides)
    return msgspec.json.encode(RepositoryResponse(type="repository", data=RepositoryAttributes(**defaults)))


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


class TestDeserializeRepository:
    def test_deserializes_valid_response(self):
        repo1 = make_repository()
        repo2 = deserialize_repository(make_repository_response(**repo1))
        assert repo1 == repo2

    def test_invalid_content_raises(self):
        with pytest.raises(SCMCodedError, match="repository_could_not_be_deserialized"):
            deserialize_repository(b"not valid json")

    def test_wrong_structure_raises(self):
        with pytest.raises(SCMCodedError, match="repository_could_not_be_deserialized"):
            deserialize_repository(msgspec.json.encode({"foo": "bar"}))


class TestFetchRepository:
    def test_success_calls_deserialize(self):
        content = make_repository_response()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = content

        with patch("scm.rpc.client.requests.get", return_value=mock_response) as mock_get:
            fetch_repository("http://localhost", "secret", 1, 1)

        call_args = mock_get.call_args
        assert "rpcsignature" in call_args.kwargs["headers"]["Authorization"]
        assert call_args.kwargs["headers"]["X-Organization-Id"] == "1"

    def test_error_response_single(self):
        error_body = msgspec.json.encode({"errors": [{"code": "repository_not_found"}]})
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.content = error_body

        with patch("scm.rpc.client.requests.get", return_value=mock_response):
            with pytest.raises(SCMCodedError, match="repository_not_found"):
                fetch_repository("http://localhost", "secret", 1, 1)

    def test_error_response_multiple(self):
        error_body = msgspec.json.encode(
            {"errors": [{"code": "repository_not_found"}, {"code": "repository_inactive"}]}
        )
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.content = error_body

        with patch("scm.rpc.client.requests.get", return_value=mock_response):
            with pytest.raises(ExceptionGroup) as exc_info:
                fetch_repository("http://localhost", "secret", 1, 1)
            assert len(exc_info.value.exceptions) == 2

    def test_undeserializable_error_response(self):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.content = b"not json"

        with patch("scm.rpc.client.requests.get", return_value=mock_response):
            with pytest.raises(SCMCodedError, match="rpc_errors_could_not_be_deserialized"):
                fetch_repository("http://localhost", "secret", 1, 1)

    def test_tuple_repository_id(self):
        content = make_repository_response()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = content

        with patch("scm.rpc.client.requests.get", return_value=mock_response) as mock_get:
            fetch_repository("http://localhost", "secret", 1, ("github", "ext-123"))

        headers = mock_get.call_args.kwargs["headers"]
        assert headers["X-Repository-Id"] == '["github","ext-123"]'


class TestFetchProvider:
    def _make_repo(self, **overrides):
        defaults = {
            "external_id": "abc",
            "integration_id": 1,
            "is_active": True,
            "name": "org/repo",
            "organization_id": 1,
            "provider_name": "github",
        }
        defaults.update(overrides)
        return defaults

    def test_github_provider(self):
        client = MagicMock()
        provider = fetch_provider(client, 1, self._make_repo(provider_name="github"))
        assert provider is not None
        assert type(provider).__name__ == "GitHubProvider"

    def test_github_enterprise_provider(self):
        client = MagicMock()
        provider = fetch_provider(client, 1, self._make_repo(provider_name="github_enterprise"))
        assert provider is not None
        assert type(provider).__name__ == "GitHubProvider"

    def test_gitlab_provider(self):
        client = MagicMock()
        provider = fetch_provider(client, 1, self._make_repo(provider_name="gitlab", external_id="abc:123"))
        assert provider is not None
        assert type(provider).__name__ == "GitLabProvider"

    def test_unknown_provider_returns_none(self):
        client = MagicMock()
        assert fetch_provider(client, 1, self._make_repo(provider_name="bitbucket")) is None


class TestRpcApiClient:
    def test_init(self):
        client = RpcApiClient(
            base_url="http://localhost",
            signing_secret="secret",
            organization_id=1,
            referrer="shared",
            repository_id=1,
        )
        assert client.base_url == "http://localhost"
        assert client.signing_secret == "secret"
        assert client.organization_id == 1
        assert client.referrer == "shared"
        assert client.repository_id == 1

    def test_request(self):
        client = RpcApiClient(
            base_url="http://localhost",
            signing_secret="secret",
            organization_id=1,
            referrer="shared",
            repository_id=1,
        )
        mock_response = MagicMock()
        client.session = MagicMock()
        client.session.post.return_value = mock_response

        result = client._request(
            method="GET",
            path="/repos/org/repo",
            headers=None,
            data=None,
            params=None,
            allow_redirects=True,
            raw_response=True,
        )

        assert result is mock_response
        call_args = client.session.post.call_args
        assert call_args.kwargs["stream"] is True
        assert "rpcsignature" in call_args.kwargs["headers"]["Authorization"]
        assert call_args.kwargs["headers"]["Content-Type"] == "application/json"
        assert call_args.kwargs["headers"]["X-Organization-Id"] == "1"
        assert call_args.kwargs["headers"]["X-Referrer"] == "shared"

        body = msgspec.json.decode(call_args.kwargs["data"])
        assert body["method"] == "GET"
        assert body["path"] == "/repos/org/repo"

    def test_request_with_tuple_repository_id(self):
        client = RpcApiClient(
            base_url="http://localhost",
            signing_secret="secret",
            organization_id=1,
            referrer="test",
            repository_id=("github", "ext-123"),
        )
        client.session = MagicMock()
        client.session.post.return_value = MagicMock()

        client._request("GET", "/path", None, None, None, True, True)

        headers = client.session.post.call_args.kwargs["headers"]
        assert headers["X-Repository-Id"] == '["github","ext-123"]'
