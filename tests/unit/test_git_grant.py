import base64
import hashlib
import hmac
import time
from collections.abc import Callable
from typing import Any

import jwt
import msgspec
import pytest
from cryptography.hazmat.primitives.asymmetric import ec, ed448, rsa
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    BestAvailableEncryption,
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
    load_pem_private_key,
    load_pem_public_key,
)

from scm.errors import GitGrantExpired, GitGrantKeyError, GitInvalidGrant
from scm.git.grant import (
    GRANT_AUDIENCE,
    generate_private_key,
    public_key_of,
    require_private_key,
    require_public_keys,
    sign_grant,
    verify_grant,
)
from scm.git.types import GRANT_MAX_LENGTH, Grant
from scm.secret import Secret
from tests.unit.error_reports import assert_not_in_frames
from tests.unit.git_builders import FAR_FUTURE, KeyPair, make_grant, make_key_pair

# Before the real time: PyJWT checks iat against the real clock, so a grant must not be issued after it.
NOW = 1_700_000_000
KEY = make_key_pair()
OLD_KEY = make_key_pair()
NEW_KEY = make_key_pair()
OTHER_KEY = make_key_pair()


def key_id(key: KeyPair) -> str:
    raw = load_pem_public_key(key.public.encode()).public_bytes(Encoding.Raw, PublicFormat.Raw)
    return hashlib.sha256(raw).hexdigest()[:16]


def claims_of(grant: Grant) -> dict:
    claims = msgspec.to_builtins(grant)
    claims["exp"] = claims.pop("expires_at")
    return claims | {"iat": NOW, "aud": GRANT_AUDIENCE}


def sign_claims(claims: dict, key: Any = KEY.key, algorithm: str = "EdDSA", kid: str | None = None) -> str:
    """Sign arbitrary claims the way sign_grant does, to reach the checks that run after the signature."""
    return jwt.encode(claims, key, algorithm=algorithm, headers={"kid": kid or key_id(KEY)})


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def b64url_decode(data: str) -> bytes:
    return base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))


def handmade_token(header: dict, claims: dict, sign: Callable[[bytes], bytes] = lambda signing_input: b"") -> str:
    """A JWT that PyJWT refuses to encode."""
    signing_input = f"{b64url(msgspec.json.encode(header))}.{b64url(msgspec.json.encode(claims))}"
    return f"{signing_input}.{b64url(sign(signing_input.encode()))}"


def assert_invalid(token: str, detail: str, public_keys: list[str] | None = None) -> None:
    with pytest.raises(GitInvalidGrant) as exc_info:
        verify_grant(public_keys or [KEY.public], token, now=NOW)
    assert detail in exc_info.value.detail


def key_lines(pem: str) -> list[str]:
    """The lines of base64 between a PEM's delimiters: the key material an error must not echo.

    Checked one by one, since an error could quote a key that spans several lines with or without its line breaks.
    """
    return [line for line in pem.splitlines() if line and not line.startswith("-----")]


def pem_body(pem: str) -> str:
    """The base64 between a PEM's delimiters, as one string."""
    return "".join(key_lines(pem))


def assert_quotes_no_key(error: BaseException, pem: str) -> None:
    """Neither the error's message nor that of the cryptography error it chains quotes the key."""
    for message in (str(error), str(error.__cause__)):
        for line in key_lines(pem):
            assert line not in message


def assert_key_not_in_frames(error: BaseException, private_key: Secret) -> None:
    """No frame of the error's traceback shows the key. The caller's frame is in the traceback too, so it must hold the
    key only as a Secret; the lines are revealed here, in a frame that is not."""
    for line in key_lines(private_key.reveal()):
        assert_not_in_frames(error, line)


class TestRoundTrip:
    def test_verifies_what_it_signed(self):
        grant = make_grant()
        assert verify_grant([KEY.public], sign_grant(KEY.private, grant, now=NOW), now=NOW) == grant

    def test_public_key_of_the_signing_key(self):
        # make_key_pair serializes the public key with cryptography directly, not through public_key_of.
        assert public_key_of(KEY.private) == KEY.public

    def test_push_grant(self):
        grant = make_grant(operation="push")
        assert verify_grant([KEY.public], sign_grant(KEY.private, grant, now=NOW), now=NOW).operation == "push"

    def test_token_is_a_jwt_with_the_grants_claims(self):
        token = sign_grant(KEY.private, make_grant(), now=NOW)
        assert jwt.get_unverified_header(token) == {"alg": "EdDSA", "typ": "JWT", "kid": key_id(KEY)}
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

    def test_key_id_is_the_same_for_every_grant_a_key_signs(self):
        # The signer derives it from its private key and the verifier from the public key, with no setting to match.
        tokens = [sign_grant(KEY.private, make_grant(organization_id=i), now=NOW) for i in (1, 2)]
        assert {jwt.get_unverified_header(token)["kid"] for token in tokens} == {key_id(KEY)}
        assert key_id(KEY) != key_id(OTHER_KEY)

    def test_public_key_with_surrounding_whitespace(self):
        # As a setting read from a file or an environment variable may have.
        token = sign_grant(KEY.private, make_grant(), now=NOW)
        assert verify_grant([f"\n  {KEY.public}\n\n"], token, now=NOW) == make_grant()


class TestGeneratePrivateKey:
    def test_signs_grants_its_public_key_verifies(self):
        private_key = generate_private_key()
        token = sign_grant(private_key, make_grant(), now=NOW)
        assert verify_grant([public_key_of(private_key)], token, now=NOW) == make_grant()

    def test_ed25519_private_key_in_pkcs8_pem(self):
        # What openssl genpkey writes, so a generated key can go wherever a signer reads its key from.
        pem = generate_private_key().reveal()
        assert pem.startswith("-----BEGIN PRIVATE KEY-----\n")
        assert isinstance(load_pem_private_key(pem.encode(), password=None), Ed25519PrivateKey)

    def test_new_key_each_time(self):
        assert generate_private_key().reveal() != generate_private_key().reveal()


class TestExpiry:
    def test_expired(self):
        with pytest.raises(GitGrantExpired, match="expired"):
            verify_grant([KEY.public], sign_grant(KEY.private, make_grant(expires_at=NOW - 1), now=NOW), now=NOW)

    def test_expires_at_its_expiry_time(self):
        with pytest.raises(GitGrantExpired):
            verify_grant([KEY.public], sign_grant(KEY.private, make_grant(expires_at=NOW), now=NOW), now=NOW)

    def test_forged_grant_is_invalid_even_when_expired(self):
        # Expiry is checked only once the signature matches, so an expired grant is always a genuine one.
        token = sign_grant(OTHER_KEY.private, make_grant(expires_at=NOW - 1), now=NOW)
        assert_invalid(token, "not signed with a known key")

    def test_valid_until_its_expiry_time(self):
        token = sign_grant(KEY.private, make_grant(expires_at=NOW + 1), now=NOW)
        assert verify_grant([KEY.public], token, now=NOW).expires_at == NOW + 1

    # PyJWT checks iat against the real clock, so these sign against it too.
    def test_issued_in_the_future(self):
        assert_invalid(sign_grant(KEY.private, make_grant(), now=time.time() + 600), "not yet valid (iat)")

    def test_issued_slightly_in_the_future(self):
        # The signer's clock may run a little ahead of the verifier's.
        token = sign_grant(KEY.private, make_grant(), now=time.time() + 30)
        assert verify_grant([KEY.public], token, now=NOW) == make_grant()

    @pytest.mark.parametrize("iat", [float(NOW), NOW + 0.5])
    def test_iat_may_be_a_float(self, iat):
        assert verify_grant([KEY.public], sign_claims(claims_of(make_grant()) | {"iat": iat}), now=NOW) == make_grant()

    def test_iat_must_be_a_number(self):
        assert_invalid(sign_claims(claims_of(make_grant()) | {"iat": "yesterday"}), "iat")

    @pytest.mark.parametrize("claim", ["exp", "iat", "aud"])
    def test_jwt_claims_are_required(self, claim):
        claims = claims_of(make_grant())
        del claims[claim]
        assert_invalid(sign_claims(claims), claim)


class TestTampering:
    def test_changed_payload(self):
        header, _, signature = sign_grant(KEY.private, make_grant(), now=NOW).split(".")
        forged = b64url(msgspec.json.encode(claims_of(make_grant(organization_id=2))))
        assert_invalid(f"{header}.{forged}.{signature}", "signature does not match")

    def test_changed_signature(self):
        header, payload, signature = sign_grant(KEY.private, make_grant(), now=NOW).split(".")
        flipped = bytearray(b64url_decode(signature))
        flipped[0] ^= 1
        assert_invalid(f"{header}.{payload}.{b64url(bytes(flipped))}", "signature does not match")

    def test_unknown_key(self):
        assert_invalid(sign_grant(OTHER_KEY.private, make_grant(), now=NOW), "not signed with a known key")

    def test_no_key_id(self):
        token = jwt.encode(claims_of(make_grant()), KEY.key, algorithm="EdDSA")
        assert_invalid(token, "not signed with a known key")

    # Ed448 is EdDSA too, so PyJWT would verify with an Ed448 key; the verifier only holds Ed25519 keys.
    @pytest.mark.parametrize("key", [OTHER_KEY.key, ed448.Ed448PrivateKey.generate()], ids=["ed25519", "ed448"])
    def test_other_key_claiming_a_known_key_id(self, key):
        assert_invalid(sign_claims(claims_of(make_grant()), key=key), "signature does not match")

    def test_token_without_an_audience(self):
        # A token signed with the same key for another purpose need not carry an audience.
        claims = claims_of(make_grant())
        del claims["aud"]
        assert_invalid(sign_claims(claims), '"aud"')

    def test_token_for_another_audience(self):
        assert_invalid(sign_claims(claims_of(make_grant()) | {"aud": "scm-rpc"}), "Audience doesn't match")


class TestAlgorithms:
    """EdDSA is the only algorithm a grant may name, whatever key its kid points to."""

    def test_hs256_keyed_with_the_public_key(self):
        # The public key is no secret, so a verifier that let a token pick HS256 and used the key it holds as the HMAC
        # key would accept a grant from anyone. PyJWT refuses to sign with a PEM as an HMAC key, so it is made here.
        token = handmade_token(
            {"alg": "HS256", "typ": "JWT", "kid": key_id(KEY)},
            claims_of(make_grant()),
            lambda signing_input: hmac.digest(KEY.public.encode(), signing_input, "sha256"),
        )
        assert_invalid(token, "alg value is not allowed")

    def test_hs256(self):
        # 32 bytes, the shortest HS256 key PyJWT signs with without warning.
        token = sign_claims(claims_of(make_grant()), key="test-only-hmac-key-for-hs256-32b", algorithm="HS256")
        assert_invalid(token, "alg value is not allowed")

    def test_unsigned_token(self):
        token = jwt.encode(claims_of(make_grant()), None, algorithm="none", headers={"kid": key_id(KEY)})
        assert_invalid(token, "alg value is not allowed")

    def test_other_asymmetric_algorithm(self):
        token = sign_claims(claims_of(make_grant()), key=ec.generate_private_key(ec.SECP256R1()), algorithm="ES256")
        assert_invalid(token, "alg value is not allowed")


def grant_with_repositories(count: int) -> Grant:
    # Names of one length, so each repository adds the same to the grant.
    return make_grant(repositories=[("github", f"getsentry/repository-{i:04d}") for i in range(count)])


class TestLength:
    """The grant travels in a request header, and sandbox-router refuses one longer than GRANT_MAX_LENGTH."""

    def test_too_long_to_sign(self):
        with pytest.raises(ValueError, match=f"over GRANT_MAX_LENGTH \\({GRANT_MAX_LENGTH}\\)") as exc_info:
            sign_grant(KEY.private, grant_with_repositories(100), now=NOW)
        # The grant is at fault rather than the key, so the signer can tell the two apart.
        assert not isinstance(exc_info.value, GitGrantKeyError)
        # The signer reports this error with its frames, which hold the private key only as a Secret and a key object.
        assert_key_not_in_frames(exc_info.value, KEY.private)

    def test_longest_grant_that_fits_signs(self):
        # Add repositories until one more would not fit.
        count = 1
        token = sign_grant(KEY.private, grant_with_repositories(count), now=NOW)
        while True:
            try:
                longer = sign_grant(KEY.private, grant_with_repositories(count + 1), now=NOW)
            except ValueError:
                break
            token, count = longer, count + 1
        # One more repository adds about 50 characters.
        assert GRANT_MAX_LENGTH - 100 < len(token) <= GRANT_MAX_LENGTH
        assert verify_grant([KEY.public], token, now=NOW) == grant_with_repositories(count)

    def test_grant_for_one_repository(self):
        # GRANT_MAX_LENGTH's docstring sizes grants from this.
        grant = make_grant(repositories=[("github", "getsentry/scm-platform")])
        assert len(sign_grant(KEY.private, grant, now=NOW)) < 450


def other_key_types() -> list:
    return [
        rsa.generate_private_key(public_exponent=65537, key_size=2048),
        ec.generate_private_key(ec.SECP256R1()),
        ed448.Ed448PrivateKey.generate(),
    ]


def private_pem(key) -> str:
    return key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()).decode()


def public_pem(key) -> str:
    return key.public_key().public_bytes(Encoding.PEM, PublicFormat.SubjectPublicKeyInfo).decode()


class TestPrivateKey:
    """A signing key that is not one is a mistake in the signer's configuration, raised as GitGrantKeyError."""

    @pytest.fixture(
        params=[sign_grant, public_key_of, require_private_key], ids=["sign_grant", "public_key_of", "require"]
    )
    def load(self, request) -> Callable[[Secret], object]:
        if request.param is sign_grant:
            return lambda private_key: sign_grant(private_key, make_grant(), now=NOW)
        return request.param

    def test_require_private_key_returns_it(self):
        assert require_private_key(KEY.private) is KEY.private

    def test_empty(self, load):
        with pytest.raises(GitGrantKeyError, match="^The git grant signing key is empty\\.$"):
            load(Secret(""))

    def test_not_a_secret(self, load):
        pem = KEY.private.reveal()
        expected = r"^The git grant signing key must be an scm\.Secret, not str\."
        with pytest.raises(GitGrantKeyError, match=expected) as exc_info:
            load(pem)
        assert_quotes_no_key(exc_info.value, pem)

    @pytest.mark.parametrize(
        "pem",
        ["not a key", KEY.public, KEY.private.reveal()[:80], pem_body(KEY.private.reveal())],
        ids=["garbage", "public-key", "truncated", "no-delimiters"],
    )
    def test_not_a_private_key(self, load, pem):
        expected = (
            r"^The git grant signing key is not an unencrypted private key in PEM \(-----BEGIN PRIVATE KEY-----\)"
        )
        with pytest.raises(GitGrantKeyError, match=expected) as exc_info:
            load(Secret(pem))
        assert_quotes_no_key(exc_info.value, pem)

    def test_encrypted(self, load):
        pem = KEY.key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, BestAvailableEncryption(b"test-only")).decode()
        with pytest.raises(GitGrantKeyError, match="not an unencrypted private key") as exc_info:
            load(Secret(pem))
        assert "encrypted" in str(exc_info.value.__cause__)
        assert_quotes_no_key(exc_info.value, pem)

    @pytest.mark.parametrize("key", other_key_types(), ids=["rsa", "ec", "ed448"])
    def test_other_key_type(self, load, key):
        private_key = Secret(private_pem(key))
        with pytest.raises(GitGrantKeyError, match=f"must be an Ed25519 key, not {type(key).__name__}") as exc_info:
            load(private_key)
        assert_quotes_no_key(exc_info.value, private_key.reveal())
        assert_key_not_in_frames(exc_info.value, private_key)


class TestPublicKeys:
    """A list of keys no grant could verify against is a mistake in configuration, not a refused grant."""

    @pytest.fixture(params=[verify_grant, require_public_keys], ids=["verify_grant", "require_public_keys"])
    def load(self, request) -> Callable[[list], object]:
        token = sign_grant(KEY.private, make_grant(), now=NOW)
        if request.param is verify_grant:
            return lambda public_keys: verify_grant(public_keys, token, now=NOW)
        return require_public_keys

    def test_require_public_keys_returns_them(self):
        assert require_public_keys((KEY.public, OTHER_KEY.public)) == [KEY.public, OTHER_KEY.public]

    def test_empty_list(self, load):
        # Under HS256, verify_grant refused every grant against an empty list; now the caller learns it has no keys.
        with pytest.raises(GitGrantKeyError, match="no git grant public keys"):
            load([])

    def test_private_key(self, load):
        pem = KEY.private.reveal()
        with pytest.raises(GitGrantKeyError, match=r"^public_keys\[1\] is a private key\.") as exc_info:
            load([OTHER_KEY.public, pem])
        assert_quotes_no_key(exc_info.value, pem)

    def test_private_key_as_a_secret(self, load):
        with pytest.raises(GitGrantKeyError, match=r"^public_keys\[0\] must be a PEM string, not Secret\."):
            load([KEY.private])

    def test_empty_key(self, load):
        # As a list read from a setting with a trailing separator may have.
        with pytest.raises(GitGrantKeyError, match=r"^public_keys\[1\] is not a public key in PEM"):
            load([KEY.public, ""])

    @pytest.mark.parametrize(
        "pem",
        ["not a key", OTHER_KEY.public[:60], pem_body(OTHER_KEY.public)],
        ids=["garbage", "truncated", "no-delimiters"],
    )
    def test_not_a_public_key(self, load, pem):
        # One bad key fails the whole list rather than being skipped, so it is noticed before it is needed.
        with pytest.raises(GitGrantKeyError, match=r"^public_keys\[1\] is not a public key in PEM") as exc_info:
            load([KEY.public, pem])
        assert_quotes_no_key(exc_info.value, pem)

    @pytest.mark.parametrize("key", other_key_types(), ids=["rsa", "ec", "ed448"])
    def test_other_key_type(self, load, key):
        pem = public_pem(key)
        expected = f"^public_keys\\[0\\] must be an Ed25519 key, not {type(key.public_key()).__name__}"
        with pytest.raises(GitGrantKeyError, match=expected) as exc_info:
            load([pem])
        assert_quotes_no_key(exc_info.value, pem)


class TestRotatedKeys:
    @pytest.mark.parametrize("signer", [OLD_KEY, NEW_KEY], ids=["old", "new"])
    def test_both_keys_verify_during_rotation(self, signer):
        token = sign_grant(signer.private, make_grant(), now=NOW)
        assert verify_grant([NEW_KEY.public, OLD_KEY.public], token, now=NOW) == make_grant()

    def test_old_key_refused_after_rotation(self):
        token = sign_grant(OLD_KEY.private, make_grant(), now=NOW)
        assert_invalid(token, "not signed with a known key", public_keys=[NEW_KEY.public])


class TestMalformed:
    @pytest.mark.parametrize("token", ["nodot", "a.b.c", "a.b", "...", "é.é.é"])
    def test_malformed_token(self, token):
        assert_invalid(token, "malformed")

    @pytest.mark.parametrize("kid", [5, ["a"], {"a": 1}])
    def test_key_id_that_is_not_a_string(self, kid):
        # PyJWT refuses it while reading the header, and the grant is refused rather than failing the request.
        assert_invalid(handmade_token({"alg": "EdDSA", "kid": kid}, claims_of(make_grant())), "malformed")

    def test_deeply_nested_header(self):
        # PyJWT reads the header before it checks the signature, so anyone can send one. Before PyJWT 2.14.0, nesting
        # deeper than json's recursion limit raised RecursionError, which failed the request (GHSA-8wjv-2p76-3863).
        depth = 100_000
        header = b64url(b'{"alg":"EdDSA","kid":' + b"[" * depth + b"]" * depth + b"}")
        assert_invalid(f"{header}.{b64url(b'{}')}.", "malformed")

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
        assert_invalid(sign_grant(KEY.private, make_grant(repositories=[]), now=NOW), "names no repositories")
