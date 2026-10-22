import base64
import hashlib
import time

import jwt
import msgspec
import pytest

from scm.errors import GitGrantExpired, GitInvalidGrant
from scm.git.grant import GRANT_AUDIENCE, sign_grant, verify_grant
from scm.git.types import Grant
from tests.unit.git_builders import FAR_FUTURE, make_grant

# Before the real time: PyJWT checks iat against the real clock, so a grant must not be issued after it.
NOW = 1_700_000_000
# HS256 secrets of at least 32 bytes, as PyJWT recommends.
SECRET = "test-secret-0000000000000000000000"
OLD_SECRET = "test-secret-old-000000000000000000"
NEW_SECRET = "test-secret-new-000000000000000000"


def key_id(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()[:8]


def claims_of(grant: Grant) -> dict:
    claims = msgspec.to_builtins(grant)
    claims["exp"] = claims.pop("expires_at")
    return claims | {"iat": NOW, "aud": GRANT_AUDIENCE}


def sign_claims(claims: dict, secret: str = SECRET, algorithm: str = "HS256", kid: str | None = None) -> str:
    """Sign arbitrary claims the way sign_grant does, to reach the checks that run after the signature."""
    return jwt.encode(claims, secret, algorithm=algorithm, headers={"kid": kid or key_id(secret)})


def assert_invalid(token: str, detail: str, secrets: list[str] | None = None) -> None:
    with pytest.raises(GitInvalidGrant) as exc_info:
        verify_grant(secrets or [SECRET], token, now=NOW)
    assert detail in exc_info.value.detail


class TestRoundTrip:
    def test_verifies_what_it_signed(self):
        grant = make_grant()
        assert verify_grant([SECRET], sign_grant(SECRET, grant, now=NOW), now=NOW) == grant

    def test_push_grant(self):
        grant = make_grant(operation="push")
        assert verify_grant([SECRET], sign_grant(SECRET, grant, now=NOW), now=NOW).operation == "push"

    def test_token_is_a_jwt_with_the_grants_claims(self):
        token = sign_grant(SECRET, make_grant(), now=NOW)
        assert jwt.get_unverified_header(token) == {"alg": "HS256", "typ": "JWT", "kid": key_id(SECRET)}
        assert jwt.decode(token, options={"verify_signature": False}) == {
            "organization_id": 1,
            "referrer": "seer.coding_job",
            "operation": "fetch",
            "repositories": [5, ["github", "123"]],
            "version": 1,
            "exp": FAR_FUTURE,
            "iat": NOW,
            "aud": GRANT_AUDIENCE,
        }


class TestExpiry:
    def test_expired(self):
        with pytest.raises(GitGrantExpired, match="expired"):
            verify_grant([SECRET], sign_grant(SECRET, make_grant(expires_at=NOW - 1), now=NOW), now=NOW)

    def test_expires_at_its_expiry_time(self):
        with pytest.raises(GitGrantExpired):
            verify_grant([SECRET], sign_grant(SECRET, make_grant(expires_at=NOW), now=NOW), now=NOW)

    def test_forged_grant_is_invalid_even_when_expired(self):
        # Expiry is checked only once the signature matches, so an expired grant is always a genuine one.
        token = sign_grant("other-secret-0000000000000000000000", make_grant(expires_at=NOW - 1), now=NOW)
        assert_invalid(token, "not signed with a known secret")

    def test_valid_until_its_expiry_time(self):
        token = sign_grant(SECRET, make_grant(expires_at=NOW + 1), now=NOW)
        assert verify_grant([SECRET], token, now=NOW).expires_at == NOW + 1

    # PyJWT checks iat against the real clock, so these sign against it too.
    def test_issued_in_the_future(self):
        assert_invalid(sign_grant(SECRET, make_grant(), now=time.time() + 600), "not yet valid (iat)")

    def test_issued_slightly_in_the_future(self):
        # The signer's clock may run a little ahead of the verifier's.
        assert verify_grant([SECRET], sign_grant(SECRET, make_grant(), now=time.time() + 30), now=NOW) == make_grant()

    @pytest.mark.parametrize("iat", [float(NOW), NOW + 0.5])
    def test_iat_may_be_a_float(self, iat):
        assert verify_grant([SECRET], sign_claims(claims_of(make_grant()) | {"iat": iat}), now=NOW) == make_grant()

    def test_iat_must_be_a_number(self):
        assert_invalid(sign_claims(claims_of(make_grant()) | {"iat": "yesterday"}), "iat")

    @pytest.mark.parametrize("claim", ["exp", "iat", "aud"])
    def test_jwt_claims_are_required(self, claim):
        claims = claims_of(make_grant())
        del claims[claim]
        assert_invalid(sign_claims(claims), claim)


class TestTampering:
    def test_changed_payload(self):
        header, _, signature = sign_grant(SECRET, make_grant(), now=NOW).split(".")
        forged = base64.urlsafe_b64encode(msgspec.json.encode(claims_of(make_grant(organization_id=2))))
        assert_invalid(f"{header}.{forged.rstrip(b'=').decode()}.{signature}", "signature does not match")

    def test_changed_signature(self):
        header, payload, signature = sign_grant(SECRET, make_grant(), now=NOW).split(".")
        flipped = bytearray(base64.urlsafe_b64decode(signature + "="))
        flipped[0] ^= 1
        resigned = base64.urlsafe_b64encode(bytes(flipped)).rstrip(b"=").decode()
        assert_invalid(f"{header}.{payload}.{resigned}", "signature does not match")

    def test_unknown_secret(self):
        assert_invalid(
            sign_grant("other-secret-0000000000000000000000", make_grant(), now=NOW), "not signed with a known"
        )

    def test_other_secret_claiming_a_known_key_id(self):
        token = sign_claims(claims_of(make_grant()), secret="other-secret-0000000000000000000000", kid=key_id(SECRET))
        assert_invalid(token, "signature does not match")

    def test_no_secrets(self):
        with pytest.raises(GitInvalidGrant):
            verify_grant([], sign_grant(SECRET, make_grant(), now=NOW), now=NOW)

    def test_token_without_an_audience(self):
        # A viewer context, for one, is an HS256 JWT signed with a shared secret and carries no audience.
        claims = claims_of(make_grant())
        del claims["aud"]
        assert_invalid(sign_claims(claims), '"aud"')

    def test_token_for_another_audience(self):
        assert_invalid(sign_claims(claims_of(make_grant()) | {"aud": "scm-rpc"}), "Audience doesn't match")

    def test_unsigned_token(self):
        token = jwt.encode(claims_of(make_grant()), None, algorithm="none", headers={"kid": key_id(SECRET)})
        assert_invalid(token, "invalid")

    @pytest.mark.filterwarnings("ignore::jwt.warnings.InsecureKeyLengthWarning")
    def test_other_algorithm(self):
        assert_invalid(sign_claims(claims_of(make_grant()), algorithm="HS512"), "invalid")


class TestSecrets:
    # PyJWT signs with an empty key, so an empty secret would let anyone sign grants.
    def test_refuses_to_sign_with_an_empty_secret(self):
        with pytest.raises(ValueError, match="empty"):
            sign_grant("", make_grant(), now=NOW)

    @pytest.mark.filterwarnings("ignore::jwt.warnings.InsecureKeyLengthWarning")
    def test_refuses_to_verify_with_an_empty_secret(self):
        token = jwt.encode(claims_of(make_grant()), "", algorithm="HS256", headers={"kid": key_id("")})
        with pytest.raises(ValueError, match="empty"):
            verify_grant([SECRET, ""], token, now=NOW)


class TestRotatedSecrets:
    @pytest.mark.parametrize("signer", [OLD_SECRET, NEW_SECRET])
    def test_both_secrets_verify_during_rotation(self, signer):
        assert (
            verify_grant([NEW_SECRET, OLD_SECRET], sign_grant(signer, make_grant(), now=NOW), now=NOW) == make_grant()
        )

    def test_old_secret_refused_after_rotation(self):
        assert_invalid(sign_grant(OLD_SECRET, make_grant(), now=NOW), "not signed with a known", secrets=[NEW_SECRET])


class TestMalformed:
    @pytest.mark.parametrize("token", ["nodot", "a.b.c", "a.b", "...", "é.é.é"])
    def test_malformed_token(self, token):
        assert_invalid(token, "malformed")

    def test_missing(self):
        assert_invalid("", "missing")

    @pytest.mark.parametrize(
        "change",
        [
            {"organization_id": True},
            {"organization_id": "1"},
            {"referrer": None},
            {"operation": "delete"},
            {"version": 2},
            {"repositories": [["github"]]},
            {"repositories": [["unknown_provider", "123"]]},
            {"exp": NOW + 600.5},
            {"narrower_than_you_think": True},
        ],
    )
    def test_signed_but_invalid_claims(self, change):
        assert_invalid(sign_claims(claims_of(make_grant()) | change), "payload is invalid")

    def test_referrer_is_required(self):
        # Without it Sentry could not tell a coding job's clone from Seer's own fetch.
        claims = claims_of(make_grant())
        del claims["referrer"]
        assert_invalid(sign_claims(claims), "referrer")

    def test_no_repositories(self):
        assert_invalid(sign_grant(SECRET, make_grant(repositories=[]), now=NOW), "names no repositories")
