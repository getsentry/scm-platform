import base64
import hashlib
import hmac
import json

import msgspec
import pytest

from scm.errors import GitInvalidGrant
from scm.git.grant import sign_grant, verify_grant
from scm.git.types import Grant
from scm.rpc.helpers import sign_post

NOW = 1_800_000_000
SECRET = "secret"


def make_grant(**overrides) -> Grant:
    fields = {
        "organization_id": 1,
        "operation": "fetch",
        "repositories": (5, ("github", "123")),
        "expires_at": NOW + 600,
    }
    fields.update(overrides)
    return Grant(**fields)  # type: ignore[arg-type]


def b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def sign_payload(payload: bytes, secret: str = SECRET) -> str:
    """Sign arbitrary bytes the way sign_grant does, to reach the checks that run after the signature."""
    signature = hmac.new(secret.encode(), b"scm-git-grant." + payload, hashlib.sha256).digest()
    return f"{b64(payload)}.{b64(signature)}"


def payload_of(grant: Grant) -> dict:
    return json.loads(msgspec.json.encode(grant))


def assert_invalid(token: str, detail: str, secrets: list[str] | None = None) -> None:
    with pytest.raises(GitInvalidGrant) as exc_info:
        verify_grant(secrets or [SECRET], token, now=NOW)
    assert detail in exc_info.value.detail


class TestRoundTrip:
    def test_verifies_what_it_signed(self):
        grant = make_grant()
        assert verify_grant([SECRET], sign_grant(SECRET, grant), now=NOW) == grant

    def test_keeps_both_repository_id_forms(self):
        grant = verify_grant([SECRET], sign_grant(SECRET, make_grant()), now=NOW)
        assert grant.repositories == (5, ("github", "123"))

    def test_push_grant(self):
        grant = make_grant(operation="push")
        assert verify_grant([SECRET], sign_grant(SECRET, grant), now=NOW).operation == "push"

    def test_payload_carries_the_version(self):
        token = sign_grant(SECRET, make_grant())
        payload = json.loads(base64.urlsafe_b64decode(token.split(".")[0] + "=="))
        assert payload["version"] == 1


class TestExpiry:
    def test_expired(self):
        assert_invalid(sign_grant(SECRET, make_grant(expires_at=NOW - 1)), "expired")

    def test_expires_at_its_expiry_time(self):
        assert_invalid(sign_grant(SECRET, make_grant(expires_at=NOW)), "expired")

    def test_valid_until_its_expiry_time(self):
        token = sign_grant(SECRET, make_grant(expires_at=NOW + 1))
        assert verify_grant([SECRET], token, now=NOW).expires_at == NOW + 1

    def test_expiry_is_required(self):
        payload = payload_of(make_grant())
        del payload["expires_at"]
        assert_invalid(sign_payload(json.dumps(payload).encode()), "expires_at")


class TestTampering:
    def test_changed_payload(self):
        token = sign_grant(SECRET, make_grant())
        _, signature = token.split(".")
        forged = msgspec.json.encode(make_grant(organization_id=2))
        assert_invalid(f"{b64(forged)}.{signature}", "signature does not match")

    def test_changed_signature(self):
        payload, signature = sign_grant(SECRET, make_grant()).split(".")
        flipped = bytearray(base64.urlsafe_b64decode(signature + "="))
        flipped[0] ^= 1
        assert_invalid(f"{payload}.{b64(bytes(flipped))}", "signature does not match")

    def test_other_secret(self):
        assert_invalid(sign_grant("other", make_grant()), "signature does not match")

    def test_no_secrets(self):
        with pytest.raises(GitInvalidGrant):
            verify_grant([], sign_grant(SECRET, make_grant()), now=NOW)

    def test_rpc_signature_over_the_same_payload_is_not_a_grant_signature(self):
        payload = msgspec.json.encode(make_grant())
        rpc_signature = bytes.fromhex(sign_post(SECRET, payload).removeprefix("rpc0:"))
        assert_invalid(f"{b64(payload)}.{b64(rpc_signature)}", "signature does not match")


class TestRotatedSecrets:
    def test_old_secret_verifies_during_rotation(self):
        assert verify_grant(["new", "old"], sign_grant("old", make_grant()), now=NOW) == make_grant()

    def test_new_secret_verifies_during_rotation(self):
        assert verify_grant(["new", "old"], sign_grant("new", make_grant()), now=NOW) == make_grant()

    def test_old_secret_refused_after_rotation(self):
        assert_invalid(sign_grant("old", make_grant()), "signature does not match", secrets=["new"])


class TestMalformed:
    @pytest.mark.parametrize(
        "token",
        [
            "nodot",
            "a.b.c",
            "abc.d+f",
            "ab=c.def",
            "abc.dé",
            ".",
        ],
    )
    def test_malformed_token(self, token):
        assert_invalid(token, "malformed")

    def test_missing(self):
        assert_invalid("", "missing")

    def test_signed_but_not_json(self):
        assert_invalid(sign_payload(b"not json"), "not JSON")

    @pytest.mark.parametrize(
        "change",
        [
            {"organization_id": True},
            {"organization_id": "1"},
            {"operation": "delete"},
            {"version": 2},
            {"repositories": [["github"]]},
            {"repositories": [["unknown_provider", "123"]]},
            {"expires_at": 1.5},
            {"narrower_than_you_think": True},
        ],
    )
    def test_signed_but_invalid_payload(self, change):
        payload = payload_of(make_grant()) | change
        assert_invalid(sign_payload(json.dumps(payload).encode()), "payload is invalid")

    def test_no_repositories(self):
        assert_invalid(sign_grant(SECRET, make_grant(repositories=())), "names no repositories")
