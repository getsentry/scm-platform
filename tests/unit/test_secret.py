import warnings

import pytest
import requests

from scm import Secret as ExportedSecret
from scm.manager import SourceCodeManager
from scm.rpc import helpers
from scm.rpc.client import RpcApiClient, fetch_repository
from scm.rpc.helpers import sign_get
from scm.rpc.server import RpcServer
from scm.secret import Secret

SECRET = "rpc-signing-secret-0000000000000000"


def assert_not_in_frames(error: BaseException, value: str) -> None:
    """Fail if any frame of the error's traceback holds ``value`` where an error report would show it.

    Sentry's SDK sends every frame's local variables, using ``__sentry_repr__`` or else ``repr()`` for each, and its
    default scrubbing only replaces variables with a few exact names, such as ``secret``.
    """
    tb = error.__traceback__
    while tb is not None:
        for name, local in tb.tb_frame.f_locals.items():
            shown = local.__sentry_repr__() if hasattr(local, "__sentry_repr__") else repr(local)
            assert value not in shown, f"{tb.tb_frame.f_code.co_name} holds it in {name}"
        tb = tb.tb_next


class TestSecret:
    def test_reveal(self):
        assert Secret(SECRET).reveal() == SECRET

    def test_hidden_from_repr_str_and_formatting(self):
        secret = Secret(SECRET)
        for shown in (repr(secret), str(secret), f"{secret}", f"{secret!r}", repr([secret]), repr({"key": secret})):
            assert SECRET not in shown
        assert repr(secret) == "Secret('[Filtered]')"

    def test_hidden_from_sentry(self):
        assert Secret(SECRET).__sentry_repr__() == "[Filtered]"

    def test_has_no_attributes_to_look_into(self):
        # With __slots__ there is no __dict__ for a serializer to walk.
        assert not hasattr(Secret(SECRET), "__dict__")

    def test_of_wraps_a_string_once(self):
        secret = Secret(SECRET)
        assert Secret.of(secret) is secret
        assert Secret.of(SECRET).reveal() == SECRET

    def test_signing_accepts_either(self):
        assert sign_get(Secret(SECRET), 1, 5) == sign_get(SECRET, 1, 5)


# Plain strings, the worst case: wrapping on entry must still keep them out of this library's frames.
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
class TestKeptOutOfErrorReports:
    def test_proxy_client_that_cannot_reach_sentry(self, monkeypatch):
        def unreachable(*args, **kwargs):
            raise requests.ConnectionError("Sentry is unreachable")

        monkeypatch.setattr(requests, "get", unreachable)
        with pytest.raises(requests.ConnectionError) as exc_info:
            SourceCodeManager.make_proxy_client(1, 5, base_url="https://sentry.example", signing_secret=SECRET)
        assert_not_in_frames(exc_info.value, SECRET)

    def test_api_client_that_cannot_reach_sentry(self):
        class Unreachable:
            def post(self, url, data, headers, stream=False):
                raise requests.ConnectionError("Sentry is unreachable")

        client = RpcApiClient(
            full_url="https://sentry.example/api/0/internal/scm-rpc/",
            signing_secret=SECRET,
            organization_id=1,
            referrer="shared",
            repository_id=5,
            session=Unreachable,
        )
        with pytest.raises(requests.ConnectionError) as exc_info:
            client.request("GET", "/repos/getsentry/sentry")
        assert_not_in_frames(exc_info.value, SECRET)

    def test_server_failing_while_it_verifies(self, monkeypatch):
        def failing_compare(a, b):
            raise RuntimeError("compare failed")

        monkeypatch.setattr(helpers.hmac, "compare_digest", failing_compare)
        server = RpcServer(
            secrets=[SECRET],
            fetch_repository=lambda organization_id, repository_id: None,
            fetch_provider=lambda organization_id, repository: None,
            record_count=lambda name, value, tags: None,
            emit_error=lambda error: None,
        )
        headers = {"Authorization": "rpcsignature rpc0:abc", "X-Organization-Id": "1", "X-Repository-Id": "5"}
        with pytest.raises(RuntimeError) as exc_info:
            server.get(headers)
        assert_not_in_frames(exc_info.value, SECRET)


def unreachable_get(*args, **kwargs):
    raise requests.ConnectionError("Sentry is unreachable")


class UnreachableSession:
    def get(self, url, headers):
        raise requests.ConnectionError("Sentry is unreachable")


def make_server(secrets):
    return RpcServer(
        secrets=secrets,
        fetch_repository=lambda organization_id, repository_id: None,
        fetch_provider=lambda organization_id, repository: None,
        record_count=lambda name, value, tags: None,
        emit_error=lambda error: None,
    )


def make_api_client(signing_secret):
    return RpcApiClient(
        full_url="https://sentry.example/api/0/internal/scm-rpc/",
        signing_secret=signing_secret,
        organization_id=1,
        referrer="shared",
        repository_id=5,
    )


class TestPlainStringDeprecated:
    def assert_warns_at_caller(self, record: pytest.WarningsRecorder, parameter: str) -> None:
        assert len(record) == 1
        assert f"Passing {parameter} as a plain str is deprecated" in str(record[0].message)
        # Points at the code that passed the string, not at this library.
        assert record[0].filename == __file__

    def test_exported_from_the_package(self):
        assert ExportedSecret is Secret

    def test_proxy_client(self, monkeypatch):
        monkeypatch.setattr(requests, "get", unreachable_get)
        with pytest.warns(DeprecationWarning) as record, pytest.raises(requests.ConnectionError):
            SourceCodeManager.make_proxy_client(1, 5, base_url="https://sentry.example", signing_secret=SECRET)
        self.assert_warns_at_caller(record, "signing_secret")

    def test_fetch_repository(self):
        with pytest.warns(DeprecationWarning) as record, pytest.raises(requests.ConnectionError):
            fetch_repository("https://sentry.example", SECRET, 1, 5, session=UnreachableSession)
        self.assert_warns_at_caller(record, "signing_secret")

    def test_api_client(self):
        with pytest.warns(DeprecationWarning) as record:
            make_api_client(SECRET)
        # make_api_client is in this file too, so the caller check still holds.
        self.assert_warns_at_caller(record, "signing_secret")

    def test_server_warns_once_for_several(self):
        with pytest.warns(DeprecationWarning) as record:
            server = make_server([SECRET, "another-secret"])
        self.assert_warns_at_caller(record, "secrets")
        assert all(isinstance(secret, Secret) for secret in server.secrets)

    def test_no_warning_for_secrets(self, monkeypatch):
        monkeypatch.setattr(requests, "get", unreachable_get)
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            with pytest.raises(requests.ConnectionError):
                SourceCodeManager.make_proxy_client(
                    1, 5, base_url="https://sentry.example", signing_secret=Secret(SECRET)
                )
            with pytest.raises(requests.ConnectionError):
                fetch_repository("https://sentry.example", Secret(SECRET), 1, 5, session=UnreachableSession)
            make_api_client(Secret(SECRET))
            make_server([Secret(SECRET)])
