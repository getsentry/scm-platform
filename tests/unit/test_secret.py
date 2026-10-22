import hashlib
import hmac

import pytest
import requests

from scm import Secret as ExportedSecret
from scm.manager import SourceCodeManager
from scm.rpc import helpers
from scm.rpc.client import RpcApiClient, fetch_repository
from scm.rpc.helpers import sign_get
from scm.rpc.server import RpcServer
from scm.secret import Secret
from tests.unit.error_reports import assert_not_in_frames

SECRET = "rpc-signing-secret-0000000000000000"


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

    def test_false_when_empty(self):
        # So an empty-secret check such as ``if not secret`` works on a Secret as on a string.
        assert not Secret("")
        assert Secret(SECRET)

    def test_exported_from_the_package(self):
        assert ExportedSecret is Secret

    def test_signs_with_the_revealed_value(self):
        assert sign_get(Secret(SECRET), 1, 5) == "rpc0:" + hmac.new(SECRET.encode(), b"15", hashlib.sha256).hexdigest()


# A revealed value kept in a variable, rather than used inline, would put the secret back in a frame.
class TestKeptOutOfErrorReports:
    def test_proxy_client_that_cannot_reach_sentry(self, monkeypatch):
        def unreachable(*args, **kwargs):
            raise requests.ConnectionError("Sentry is unreachable")

        monkeypatch.setattr(requests, "get", unreachable)
        with pytest.raises(requests.ConnectionError) as exc_info:
            SourceCodeManager.make_proxy_client(1, 5, base_url="https://sentry.example", signing_secret=Secret(SECRET))
        assert_not_in_frames(exc_info.value, SECRET)

    def test_api_client_that_cannot_reach_sentry(self):
        class Unreachable:
            def post(self, url, data, headers, stream=False):
                raise requests.ConnectionError("Sentry is unreachable")

        client = RpcApiClient(
            full_url="https://sentry.example/api/0/internal/scm-rpc/",
            signing_secret=Secret(SECRET),
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
            secrets=[Secret(SECRET)],
            fetch_repository=lambda organization_id, repository_id: None,
            fetch_provider=lambda organization_id, repository: None,
            record_count=lambda name, value, tags: None,
            emit_error=lambda error: None,
        )
        headers = {"Authorization": "rpcsignature rpc0:abc", "X-Organization-Id": "1", "X-Repository-Id": "5"}
        with pytest.raises(RuntimeError) as exc_info:
            server.get(headers)
        assert_not_in_frames(exc_info.value, SECRET)


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


class TestRequiresSecret:
    @pytest.mark.parametrize(
        ("call", "parameter"),
        [
            (
                lambda secret: SourceCodeManager.make_proxy_client(
                    1, 5, base_url="https://sentry.example", signing_secret=secret
                ),
                "signing_secret",
            ),
            (lambda secret: fetch_repository("https://sentry.example", secret, 1, 5), "signing_secret"),
            (make_api_client, "signing_secret"),
            (lambda secret: make_server([Secret(SECRET), secret]), "secrets"),
            (lambda secret: helpers.sign_get(secret, 1, 5), "secret"),
            (lambda secret: helpers.sign_post(secret, b"body"), "secret"),
            (lambda secret: helpers.sign(secret, b"body"), "secret"),
            (lambda secret: helpers.verify_get([secret], 1, 5, "rpc0:abc"), "secrets"),
            (lambda secret: helpers.verify_post([secret], b"body", "rpc0:abc"), "secrets"),
            (lambda secret: helpers.verify([secret], b"body", "rpc0:abc"), "secrets"),
        ],
        ids=[
            "make_proxy_client",
            "fetch_repository",
            "RpcApiClient",
            "RpcServer",
            "sign_get",
            "sign_post",
            "sign",
            "verify_get",
            "verify_post",
            "verify",
        ],
    )
    def test_refuses_a_plain_string(self, call, parameter):
        # Refused before anything is sent, so no request is made.
        with pytest.raises(TypeError, match=rf"^{parameter} must be a scm\.Secret, not str\.") as exc_info:
            call(SECRET)
        assert SECRET not in str(exc_info.value)
