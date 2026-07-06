from collections.abc import Collection
from unittest.mock import MagicMock, patch

import msgspec
import pytest
from requests.exceptions import ConnectionError as RequestsConnectionError

from scm.errors import ErrorCode, ResourceServiceUnavailable, SCMCodedError
from scm.providers.github.provider import GitHubProvider
from scm.providers.gitlab.provider import GitLabProvider
from scm.rpc.client import (
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
        with pytest.raises(SCMCodedError) as exc_info:
            fetch_repository("http://base", "secret", 1, 1)
        assert exc_info.value.code == "repository_not_found"

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
        with pytest.raises(SCMCodedError) as exc_info:
            fetch_repository("http://base", "secret", 1, 1)
        assert exc_info.value.code == "rpc_errors_could_not_be_deserialized"

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
        with pytest.raises(SCMCodedError) as exc_info:
            deserialize_repository(b"not valid json")
        assert exc_info.value.code == "repository_could_not_be_deserialized"

    def test_wrong_structure_raises(self):
        with pytest.raises(SCMCodedError) as exc_info:
            deserialize_repository(b'{"type": "unknown", "data": {}}')
        assert exc_info.value.code == "repository_could_not_be_deserialized"


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
        with pytest.raises(SCMCodedError) as exc_info:
            fetch_provider(client, 1, repo)
        assert exc_info.value.code == "rpc_invalid_grant"

    def test_gitlab_returns_gitlab_provider(self):
        client = MagicMock()
        repo = make_repository(provider_name="gitlab", external_id="gitlab.com:12345")
        provider = fetch_provider(client, 1, repo)
        assert isinstance(provider, GitLabProvider)

    def test_unknown_provider_returns_none(self):
        client = MagicMock()
        repo = make_repository(provider_name="not-a-provider")
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
    """A transient blip between us and the proxy surfaces either as a transport ``ConnectionError``
    or as a 503/504 from the gateway. Idempotent reads retry with backoff; writes never do."""

    def _make_client(
        self,
        max_transport_retries: int = 2,
        transport_retry_backoff_seconds: float = 0.25,
        status_codes: Collection[int] = (503, 504),
        retry_connection_errors: bool = True,
    ) -> RpcApiClient:
        # Retries are off by default in the library; these tests opt in to exercise the retry path.
        client = RpcApiClient(
            full_url="http://base/api/0/internal/scm-rpc",
            signing_secret="secret",
            organization_id=1,
            referrer="shared",
            repository_id=1,
            retry={
                "max_retries": max_transport_retries,
                "backoff_seconds": transport_retry_backoff_seconds,
                "status_codes": status_codes,
                "retry_connection_errors": retry_connection_errors,
            },
            record_count=MagicMock(),
        )
        client.session = MagicMock()
        return client

    @staticmethod
    def _metric_names(client: RpcApiClient) -> list[str]:
        return [call.args[0] for call in client.record_count.call_args_list]  # type: ignore[attr-defined]

    @patch("scm.rpc.client.time.sleep")
    def test_retries_connection_error_then_succeeds(self, mock_sleep):
        client = self._make_client()
        ok = MagicMock(status_code=200)
        client.session.post.side_effect = [RequestsConnectionError("connection reset"), ok]

        result = client.request(method="GET", path="/repos/org/repo/git/trees/abc")

        assert result is ok
        assert client.session.post.call_count == 2
        mock_sleep.assert_called_once()
        assert self._metric_names(client) == [
            "sentry.scm.rpc.client.transport_retry",
            "sentry.scm.rpc.client.transport_retry_recovered",
        ]
        recovered_tags = client.record_count.call_args_list[-1].args[2]  # type: ignore[attr-defined]
        assert recovered_tags == {"method": "GET", "reason": "connection_error"}

    @patch("scm.rpc.client.time.sleep")
    def test_retries_503_then_succeeds(self, mock_sleep):
        client = self._make_client()
        unavailable = MagicMock(status_code=503)
        ok = MagicMock(status_code=200)
        client.session.post.side_effect = [unavailable, ok]

        result = client.request(method="GET", path="/repos/org/repo/git/trees/abc")

        assert result is ok
        assert client.session.post.call_count == 2
        retry_tags = client.record_count.call_args_list[0].args[2]  # type: ignore[attr-defined]
        assert retry_tags == {"method": "GET", "reason": "status_503"}
        assert self._metric_names(client) == [
            "sentry.scm.rpc.client.transport_retry",
            "sentry.scm.rpc.client.transport_retry_recovered",
        ]
        recovered_tags = client.record_count.call_args_list[-1].args[2]  # type: ignore[attr-defined]
        assert recovered_tags == {"method": "GET", "reason": "status_503"}

    @patch("scm.rpc.client.time.sleep")
    def test_recovered_reason_reports_last_retry_trigger(self, mock_sleep):
        # A mixed-reason sequence (connection error, then 503, then success) tags the recovery with
        # the trigger of the final retry — there is no single reason for the whole sequence.
        client = self._make_client(max_transport_retries=3, retry_connection_errors=True)
        unavailable = MagicMock(status_code=503)
        ok = MagicMock(status_code=200)
        client.session.post.side_effect = [RequestsConnectionError("reset"), unavailable, ok]

        result = client.request(method="GET", path="/repos/org/repo/git/trees/abc")

        assert result is ok
        recovered_tags = client.record_count.call_args_list[-1].args[2]  # type: ignore[attr-defined]
        assert recovered_tags == {"method": "GET", "reason": "status_503"}

    @patch("scm.rpc.client.time.sleep")
    def test_returns_final_503_after_exhausting_retries(self, mock_sleep):
        client = self._make_client(max_transport_retries=2)
        unavailable = MagicMock(status_code=503)
        client.session.post.return_value = unavailable

        # The status is handed back unchanged; the provider maps it to a coded error.
        result = client.request(method="GET", path="/repos/org/repo/git/trees/abc")

        assert result is unavailable
        # initial attempt + 2 retries
        assert client.session.post.call_count == 3
        assert self._metric_names(client) == [
            "sentry.scm.rpc.client.transport_retry",
            "sentry.scm.rpc.client.transport_retry",
            "sentry.scm.rpc.client.transport_retry_exhausted",
        ]

    @patch("scm.rpc.client.time.sleep")
    def test_raises_service_unavailable_after_exhausting_connection_errors(self, mock_sleep):
        client = self._make_client(max_transport_retries=2)
        client.session.post.side_effect = RequestsConnectionError("connection reset")

        with pytest.raises(ResourceServiceUnavailable):
            client.request(method="GET", path="/repos/org/repo/git/trees/abc")

        assert client.session.post.call_count == 3
        assert self._metric_names(client) == [
            "sentry.scm.rpc.client.transport_retry",
            "sentry.scm.rpc.client.transport_retry",
            "sentry.scm.rpc.client.transport_retry_exhausted",
        ]

    @patch("scm.rpc.client.time.sleep")
    def test_does_not_retry_connection_error_on_non_idempotent_method(self, mock_sleep):
        client = self._make_client()
        client.session.post.side_effect = RequestsConnectionError("connection reset")

        # A write may have landed upstream, so the raw error propagates untouched and unretried.
        with pytest.raises(RequestsConnectionError):
            client.request(method="POST", path="/repos/org/repo/pulls/1/reviews", data={"body": "x"})

        assert client.session.post.call_count == 1
        mock_sleep.assert_not_called()
        assert self._metric_names(client) == []

    @patch("scm.rpc.client.time.sleep")
    def test_does_not_retry_503_on_non_idempotent_method(self, mock_sleep):
        client = self._make_client()
        unavailable = MagicMock(status_code=503)
        client.session.post.return_value = unavailable

        result = client.request(method="POST", path="/repos/org/repo/pulls/1/reviews", data={"body": "x"})

        assert result is unavailable
        assert client.session.post.call_count == 1
        mock_sleep.assert_not_called()
        assert self._metric_names(client) == []

    @patch("scm.rpc.client.time.sleep")
    def test_no_retries_by_default(self, mock_sleep):
        # The library default intercepts nothing: a consumer must opt in. An idempotent read that
        # hits a ConnectionError surfaces the raw error untouched — not re-sent, not reclassified,
        # and no transport_retry metric fires.
        client = RpcApiClient(
            full_url="http://base/api/0/internal/scm-rpc",
            signing_secret="secret",
            organization_id=1,
            referrer="shared",
            repository_id=1,
            record_count=MagicMock(),
        )
        client.session = MagicMock()
        client.session.post.side_effect = RequestsConnectionError("connection reset")

        with pytest.raises(RequestsConnectionError):
            client.request(method="GET", path="/repos/org/repo/git/trees/abc")

        assert client.session.post.call_count == 1
        mock_sleep.assert_not_called()
        assert self._metric_names(client) == []

    @patch("scm.rpc.client.time.sleep")
    def test_connection_error_not_intercepted_when_disabled(self, mock_sleep):
        # A caller can opt into status-code retries while leaving connection errors alone. The raw
        # ConnectionError then propagates untouched even for an idempotent read.
        client = self._make_client(retry_connection_errors=False)
        client.session.post.side_effect = RequestsConnectionError("connection reset")

        with pytest.raises(RequestsConnectionError):
            client.request(method="GET", path="/repos/org/repo/git/trees/abc")

        assert client.session.post.call_count == 1
        mock_sleep.assert_not_called()
        assert self._metric_names(client) == []

    @patch("scm.rpc.client.time.sleep")
    def test_reclassifies_connection_error_without_retries(self, mock_sleep):
        # Opting into connection-error interception with max_retries=0 is a coherent config: convert
        # the error to a typed, RPC-serializable one without re-sending. No retry happened, so no
        # transport_retry metric fires.
        client = self._make_client(max_transport_retries=0, retry_connection_errors=True)
        client.session.post.side_effect = RequestsConnectionError("connection reset")

        with pytest.raises(ResourceServiceUnavailable):
            client.request(method="GET", path="/repos/org/repo/git/trees/abc")

        assert client.session.post.call_count == 1
        mock_sleep.assert_not_called()
        assert self._metric_names(client) == []

    @patch("scm.rpc.client.time.sleep")
    def test_connection_error_intercept_defaults_off_when_omitted(self, mock_sleep):
        # retry_connection_errors is optional; omitting it leaves connection errors untouched.
        client = RpcApiClient(
            full_url="http://base/api/0/internal/scm-rpc",
            signing_secret="secret",
            organization_id=1,
            referrer="shared",
            repository_id=1,
            retry={"max_retries": 2, "backoff_seconds": 0.25, "status_codes": {503}},
            record_count=MagicMock(),
        )
        client.session = MagicMock()
        client.session.post.side_effect = RequestsConnectionError("connection reset")

        with pytest.raises(RequestsConnectionError):
            client.request(method="GET", path="/repos/org/repo/git/trees/abc")

        assert client.session.post.call_count == 1
        mock_sleep.assert_not_called()
        assert self._metric_names(client) == []

    @patch("scm.rpc.client.time.sleep")
    def test_no_exhausted_metric_for_status_without_retries(self, mock_sleep):
        # With retries off, a single 503 is handed back as-is. Nothing was retried, so it must not
        # be mislabeled as a post-retry exhaustion.
        client = self._make_client(max_transport_retries=0)
        unavailable = MagicMock(status_code=503)
        client.session.post.return_value = unavailable

        result = client.request(method="GET", path="/repos/org/repo/git/trees/abc")

        assert result is unavailable
        assert client.session.post.call_count == 1
        mock_sleep.assert_not_called()
        assert self._metric_names(client) == []

    @patch("scm.rpc.client.time.sleep")
    def test_uses_configured_backoff(self, mock_sleep):
        client = self._make_client(max_transport_retries=2, transport_retry_backoff_seconds=1.5)
        unavailable = MagicMock(status_code=503)
        ok = MagicMock(status_code=200)
        client.session.post.side_effect = [unavailable, unavailable, ok]

        client.request(method="GET", path="/repos/org/repo/git/trees/abc")

        # Exponential backoff off the configured base: base * 2**attempt.
        assert [call.args[0] for call in mock_sleep.call_args_list] == [1.5, 3.0]

    @patch("scm.rpc.client.time.sleep")
    def test_retries_configured_status_codes_only(self, mock_sleep):
        # A consumer can pick which gateway statuses are retriable. Here 429 is in, 503 is out.
        client = self._make_client(status_codes={429})
        rate_limited = MagicMock(status_code=429)
        ok = MagicMock(status_code=200)
        client.session.post.side_effect = [rate_limited, ok]

        result = client.request(method="GET", path="/repos/org/repo/git/trees/abc")

        assert result is ok
        assert client.session.post.call_count == 2
        retry_tags = client.record_count.call_args_list[0].args[2]  # type: ignore[attr-defined]
        assert retry_tags == {"method": "GET", "reason": "status_429"}

    @patch("scm.rpc.client.time.sleep")
    def test_does_not_retry_status_code_outside_config(self, mock_sleep):
        # 503 is not in the configured set, so it is handed back unretried.
        client = self._make_client(status_codes={429})
        unavailable = MagicMock(status_code=503)
        client.session.post.return_value = unavailable

        result = client.request(method="GET", path="/repos/org/repo/git/trees/abc")

        assert result is unavailable
        assert client.session.post.call_count == 1
        mock_sleep.assert_not_called()
        assert self._metric_names(client) == []
