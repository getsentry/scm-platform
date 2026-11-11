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
    key_id,
    public_key_of,
    require_private_key,
    require_public_keys,
    sign_grant,
    verify_grant,
)
from scm.git.types import GRANT_MAX_LENGTH, FetchRule, Grant, PushRule
from scm.secret import Secret
from tests.unit.error_reports import assert_not_in_frames
from tests.unit.git_builders import FAR_FUTURE, VERSION_1_PUSH_CLAIMS, KeyPair, make_grant, make_key_pair

# Before the real time: PyJWT checks iat against the real clock, so a grant must not be issued after it.
NOW = 1_700_000_000
KEY = make_key_pair()
OLD_KEY = make_key_pair()
NEW_KEY = make_key_pair()
OTHER_KEY = make_key_pair()


def expected_key_id(key: KeyPair) -> str:
    """The kid derived here from the raw key, so the tests check key_id and sign_grant against it."""
    raw = load_pem_public_key(key.public.encode()).public_bytes(Encoding.Raw, PublicFormat.Raw)
    return hashlib.sha256(raw).hexdigest()[:16]


def claims_of(grant: Grant) -> dict:
    claims = msgspec.to_builtins(grant)
    claims["exp"] = claims.pop("expires_at")
    return claims | {"iat": NOW, "aud": GRANT_AUDIENCE}


def sign_claims(claims: dict, key: Any = KEY.key, algorithm: str = "EdDSA", kid: str | None = None) -> str:
    """Sign arbitrary claims the way sign_grant does, to reach the checks that run after the signature."""
    return jwt.encode(claims, key, algorithm=algorithm, headers={"kid": kid or expected_key_id(KEY)})


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

    def test_rules_of_each_kind(self):
        grant = make_grant(
            rules=[
                FetchRule(repository=5, branches=["main", "seer/fix"]),
                PushRule(repository=5, branches=["seer/fix"]),
                FetchRule(repository=("gitlab", "gitlab.com:7"), any_object=True),
            ]
        )
        verified = verify_grant([KEY.public], sign_grant(KEY.private, grant, now=NOW), now=NOW)
        assert verified == grant
        assert [type(rule) for rule in verified.rules] == [FetchRule, PushRule, FetchRule]

    def test_token_is_a_jwt_with_the_grants_claims(self):
        token = sign_grant(KEY.private, make_grant(), now=NOW)
        assert jwt.get_unverified_header(token) == {"alg": "EdDSA", "typ": "JWT", "kid": expected_key_id(KEY)}
        assert jwt.decode(token, options={"verify_signature": False}) == {
            "organization_id": 1,
            "referrer": "seer.coding_job",
            "rules": [
                {"operation": "fetch", "repository": 5, "any_object": True},
                {"operation": "fetch", "repository": ["github", "123"], "any_object": True},
            ],
            "version": 2,
            "exp": FAR_FUTURE,
            "iat": NOW,
            "aud": GRANT_AUDIENCE,
        }

    def test_key_id_is_the_same_for_every_grant_a_key_signs(self):
        # The signer derives it from its private key and the verifier from the public key, with no setting to match.
        tokens = [sign_grant(KEY.private, make_grant(organization_id=i), now=NOW) for i in (1, 2)]
        assert {jwt.get_unverified_header(token)["kid"] for token in tokens} == {expected_key_id(KEY)}
        assert expected_key_id(KEY) != expected_key_id(OTHER_KEY)

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
            {"alg": "HS256", "typ": "JWT", "kid": expected_key_id(KEY)},
            claims_of(make_grant()),
            lambda signing_input: hmac.digest(KEY.public.encode(), signing_input, "sha256"),
        )
        assert_invalid(token, "alg value is not allowed")

    def test_hs256(self):
        # 32 bytes, the shortest HS256 key PyJWT signs with without warning.
        token = sign_claims(claims_of(make_grant()), key="test-only-hmac-key-for-hs256-32b", algorithm="HS256")
        assert_invalid(token, "alg value is not allowed")

    def test_unsigned_token(self):
        token = jwt.encode(claims_of(make_grant()), None, algorithm="none", headers={"kid": expected_key_id(KEY)})
        assert_invalid(token, "alg value is not allowed")

    def test_other_asymmetric_algorithm(self):
        token = sign_claims(claims_of(make_grant()), key=ec.generate_private_key(ec.SECP256R1()), algorithm="ES256")
        assert_invalid(token, "alg value is not allowed")


def grant_with_repositories(count: int) -> Grant:
    # Names of one length, so each rule adds the same to the grant.
    return make_grant(
        rules=[FetchRule(repository=("github", f"getsentry/repository-{i:04d}"), any_object=True) for i in range(count)]
    )


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
        # One more rule adds about 120 characters.
        assert GRANT_MAX_LENGTH - 150 < len(token) <= GRANT_MAX_LENGTH
        assert verify_grant([KEY.public], token, now=NOW) == grant_with_repositories(count)

    def test_grant_for_one_repository(self):
        # GRANT_MAX_LENGTH's docstring sizes grants from this.
        grant = make_grant(rules=[FetchRule(repository=("github", "getsentry/scm-platform"), any_object=True)])
        assert len(sign_grant(KEY.private, grant, now=NOW)) < 500


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


class TestKeyId:
    def test_kid_of_the_grants_the_key_signs(self):
        token = sign_grant(KEY.private, make_grant(), now=NOW)
        assert key_id(KEY.public) == jwt.get_unverified_header(token)["kid"] == expected_key_id(KEY)

    def test_signer_gets_its_own_from_its_private_key(self):
        private_key = generate_private_key()
        token = sign_grant(private_key, make_grant(), now=NOW)
        assert key_id(public_key_of(private_key)) == jwt.get_unverified_header(token)["kid"]

    def test_differs_between_keys(self):
        assert key_id(KEY.public) != key_id(OTHER_KEY.public)

    def test_public_key_with_surrounding_whitespace(self):
        assert key_id(f"\n  {KEY.public}\n\n") == key_id(KEY.public)

    def test_private_key(self):
        pem = KEY.private.reveal()
        with pytest.raises(GitGrantKeyError, match=r"^The git grant public key is a private key\.") as exc_info:
            key_id(pem)
        assert_quotes_no_key(exc_info.value, pem)

    def test_private_key_as_a_secret(self):
        with pytest.raises(GitGrantKeyError, match=r"^The git grant public key must be a PEM string, not Secret\."):
            key_id(KEY.private)  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "pem",
        ["", "not a key", OTHER_KEY.public[:60], pem_body(OTHER_KEY.public)],
        ids=["empty", "garbage", "truncated", "no-delimiters"],
    )
    def test_not_a_public_key(self, pem):
        with pytest.raises(GitGrantKeyError, match=r"^The git grant public key is not a public key in PEM") as exc_info:
            key_id(pem)
        assert_quotes_no_key(exc_info.value, pem)

    @pytest.mark.parametrize("key", other_key_types(), ids=["rsa", "ec", "ed448"])
    def test_other_key_type(self, key):
        pem = public_pem(key)
        expected = f"^The git grant public key must be an Ed25519 key, not {type(key.public_key()).__name__}"
        with pytest.raises(GitGrantKeyError, match=expected) as exc_info:
            key_id(pem)
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
            {"rules": None},
            {"rules": [{"operation": "fetch", "repository": ["github"], "any_object": True}]},
            {"rules": [{"operation": "fetch", "repository": ["unknown_provider", "123"], "any_object": True}]},
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

    @pytest.mark.parametrize("version", [1, 3, "2", None])
    def test_version_other_than_2(self, version):
        assert_invalid(sign_claims(claims_of(make_grant()) | {"version": version}), "not version 2")

    def test_without_a_version(self):
        claims = claims_of(make_grant())
        del claims["version"]
        assert_invalid(sign_claims(claims), "not version 2")

    def test_version_1_grant(self):
        assert_invalid(sign_claims(VERSION_1_PUSH_CLAIMS), "not version 2")

    def test_version_1_claims_labelled_version_2(self):
        assert_invalid(sign_claims(VERSION_1_PUSH_CLAIMS | {"version": 2}), "payload is invalid")


FETCH_NAMES_NOTHING = "A fetch rule must set branches, branch_patterns, tags, tag_patterns or any_object"
PUSH_NAMES_NOTHING = "A push rule must set branches, branch_patterns, tags or tag_patterns"


def rule_claims(*rules: dict) -> dict:
    return claims_of(make_grant()) | {"rules": list(rules)}


class TestRules:
    """A grant's rules, checked both where a grant is built and where it is verified, whoever signed it."""

    def test_fetch_and_push_rules_for_one_repository(self):
        grant = make_grant(
            rules=[
                FetchRule(repository=5, branches=["main"]),
                PushRule(repository=5, branches=["seer/fix"]),
            ]
        )
        assert verify_grant([KEY.public], sign_grant(KEY.private, grant, now=NOW), now=NOW) == grant

    def test_claims_hold_only_what_a_rule_sets(self):
        # Shorter, and a reader sees which fetch rules reach any object.
        grant = make_grant(
            rules=[
                FetchRule(repository=5, branches=["main"], tags=["v1"]),
                FetchRule(repository=6, any_object=True),
                PushRule(repository=5, branches=["seer/fix"]),
                PushRule(repository=5, branch_patterns=["seer/.+"], create_only=True),
                PushRule(repository=5, tag_patterns=["v[0-9]+"], create_only=True),
            ]
        )
        claims = jwt.decode(sign_grant(KEY.private, grant, now=NOW), options={"verify_signature": False})
        assert claims["rules"] == [
            {"operation": "fetch", "repository": 5, "branches": ["main"], "tags": ["v1"]},
            {"operation": "fetch", "repository": 6, "any_object": True},
            {"operation": "push", "repository": 5, "branches": ["seer/fix"]},
            {"operation": "push", "repository": 5, "branch_patterns": ["seer/.+"], "create_only": True},
            {"operation": "push", "repository": 5, "tag_patterns": ["v[0-9]+"], "create_only": True},
        ]

    @pytest.mark.parametrize("operation", ["fetch", "push"])
    def test_same_ref_fields_for_fetch_and_push(self, operation):
        rule = {
            "operation": operation,
            "repository": 5,
            "branches": ["main"],
            "tag_patterns": ["v[0-9]+"],
        }
        [verified] = verify_grant([KEY.public], sign_claims(rule_claims(rule)), now=NOW).rules
        assert verified.covers("refs/heads/main")
        assert verified.covers("refs/tags/v2")

    @pytest.mark.parametrize(
        "rule,error",
        [
            ({"operation": "fetch", "repository": 5}, FETCH_NAMES_NOTHING),
            ({"operation": "fetch", "repository": 5, "any_object": False}, FETCH_NAMES_NOTHING),
            ({"operation": "fetch", "repository": 5, "branches": None}, FETCH_NAMES_NOTHING),
            (
                {"operation": "fetch", "repository": 5, "branches": ["main"], "any_object": True},
                "that sets any_object must not set branches, branch_patterns, tags or tag_patterns",
            ),
            (
                {"operation": "fetch", "repository": 5, "tag_patterns": ["v.+"], "any_object": True},
                "that sets any_object must not set branches, branch_patterns, tags or tag_patterns",
            ),
            ({"operation": "fetch", "repository": 5, "branches": []}, "must not be empty"),
            ({"operation": "fetch", "repository": 5, "tags": []}, "must not be empty"),
            ({"operation": "push", "repository": 5, "branches": []}, "must not be empty"),
            ({"operation": "push", "repository": 5, "tags": []}, "must not be empty"),
            ({"operation": "push", "repository": 5}, PUSH_NAMES_NOTHING),
            ({"operation": "push", "repository": 5, "branches": None}, PUSH_NAMES_NOTHING),
            ({"operation": "push", "repository": 5, "create_only": True}, PUSH_NAMES_NOTHING),
            (
                {"operation": "push", "repository": 5, "branches": ["a"], "branch_patterns": ["a"]},
                "must not set both branches and branch_patterns",
            ),
            (
                {"operation": "fetch", "repository": 5, "tags": ["a"], "tag_patterns": ["a"]},
                "must not set both tags and tag_patterns",
            ),
            ({"operation": "push", "repository": 5, "branch_patterns": []}, "must not be empty"),
            ({"operation": "push", "repository": 5, "tag_patterns": []}, "must not be empty"),
            ({"operation": "push", "repository": 5, "branch_patterns": [""]}, "must not be empty"),
            ({"operation": "fetch", "repository": 5, "tag_patterns": [""]}, "must not be empty"),
            ({"operation": "push", "repository": 5, "branch_patterns": ["seer/("]}, "is not a regular expression"),
            ({"operation": "fetch", "repository": 5, "tag_patterns": ["v("]}, "is not a regular expression"),
            ({"operation": "push", "repository": 5, "branches": ["a"], "create_only": 1}, "Expected `bool`"),
            (
                {"operation": "push", "repository": 5, "branches": ["a"], "any_object": True},
                "unknown field `any_object`",
            ),
            ({"operation": "fetch", "repository": 5, "any_object": True, "create_only": True}, "unknown field"),
            ({"operation": "fetch", "repository": 5, "branches": ["main"], "deepen": 50}, "unknown field `deepen`"),
            ({"operation": "push", "repository": 5, "refs": ["refs/heads/main"]}, "unknown field `refs`"),
            ({"operation": "delete", "repository": 5, "branches": ["main"]}, "Invalid value 'delete'"),
            ({"repository": 5, "any_object": True}, "missing required field `operation`"),
            ({"operation": "fetch", "any_object": True}, "missing required field `repository`"),
        ],
    )
    def test_invalid_rule(self, rule, error):
        assert_invalid(sign_claims(rule_claims(rule)), error)

    def test_rule_without_refs_or_any_object_cannot_be_built(self):
        with pytest.raises(ValueError, match=FETCH_NAMES_NOTHING):
            FetchRule(repository=5)
        with pytest.raises(ValueError, match=PUSH_NAMES_NOTHING):
            PushRule(repository=5, create_only=True)

    def test_rule_with_refs_and_any_object_cannot_be_built(self):
        with pytest.raises(ValueError, match="that sets any_object must not set"):
            FetchRule(repository=5, tags=["v1"], any_object=True)

    @pytest.mark.parametrize("make_rule", [FetchRule, PushRule], ids=["fetch", "push"])
    @pytest.mark.parametrize("field", ["branches", "branch_patterns", "tags", "tag_patterns"])
    def test_rule_with_an_empty_list_cannot_be_built(self, make_rule, field):
        with pytest.raises(ValueError, match="must not be empty"):
            make_rule(repository=5, **{field: []})

    def test_no_rules(self):
        with pytest.raises(ValueError, match="at least one rule"):
            make_grant(rules=[])
        assert_invalid(sign_claims(rule_claims()), "at least one rule")

    def test_several_rules_for_one_repository_and_operation(self):
        # A request passes if any of them allows it, so their order doesn't matter.
        grant = make_grant(
            rules=[
                FetchRule(repository=5, any_object=True),
                FetchRule(repository=5, branches=["main"]),
                PushRule(repository=5, branch_patterns=["seer/.+"], create_only=True),
                PushRule(repository=5, branches=["seer/fix"]),
            ]
        )
        assert verify_grant([KEY.public], sign_grant(KEY.private, grant, now=NOW), now=NOW) == grant
        assert grant.fetch_rules(5) == grant.rules[:2]
        assert grant.push_rules(5) == grant.rules[2:]
        assert grant.push_rules(("github", "123")) == []

    def test_one_repository_named_both_ways(self):
        # Nothing matches a database id to a provider id, so the two are separate repositories here, as in requests.
        rules = [
            FetchRule(repository=5, any_object=True),
            FetchRule(repository=("github", "123"), branches=["main"]),
        ]
        assert make_grant(rules=rules).rules == rules


# Invalid as a branch name and as a tag name.
INVALID_REF_NAMES = [
    "",
    "refs/heads/main",
    "refs/tags/v1",
    "-main",
    "HEAD",
    "@",
    "a..b",
    "a b",
    "a\tb",
    "a\x00b",
    "a\x7fb",
    "a~1",
    "a^",
    "a:b",
    "a?",
    "a*",
    "a[b",
    "a\\b",
    "/main",
    "main/",
    "a//b",
    ".main",
    "a/.b",
    "main.lock",
    "a/b.lock/c",
    "main.",
    "a@{1}",
]


class TestCovers:
    """A rule covers full ref names: its branches under refs/heads/ and its tags under refs/tags/."""

    @pytest.mark.parametrize(
        "patterns,ref,covered",
        [
            (["seer/.+"], "refs/heads/seer/fix", True),
            (["seer/.+"], "refs/heads/seer/fix/deeper", True),
            (["seer/.+"], "refs/heads/seer/", False),
            # Matched against the whole name, not searched for in it.
            (["seer/.+"], "refs/heads/main-seer/fix", False),
            (["seer/[a-z-]+"], "refs/heads/seer/fix/deeper", False),
            (["seer"], "refs/heads/seer/fix", False),
            (["seer/[a-z]+", "cursor/.+"], "refs/heads/cursor/x", True),
            (["seer/.+"], "refs/heads/Seer/fix", False),
            # A branch pattern covers only branches.
            (["seer/.+"], "refs/tags/seer/fix", False),
            (["seer/.+"], "seer/fix", False),
        ],
    )
    def test_branch_patterns(self, patterns, ref, covered):
        assert PushRule(repository=5, branch_patterns=patterns).covers(ref) is covered

    @pytest.mark.parametrize(
        "ref,covered",
        [
            ("refs/tags/v1.2", True),
            ("refs/tags/v1", False),
            ("refs/tags/x/v1.2", False),
            ("refs/heads/v1.2", False),
        ],
    )
    def test_tag_patterns(self, ref, covered):
        assert PushRule(repository=5, tag_patterns=[r"v[0-9]+\.[0-9]+"]).covers(ref) is covered

    def test_listed_refs(self):
        rule = PushRule(repository=5, branches=["seer/fix"], tags=["v1"])
        assert rule.refs == ["refs/heads/seer/fix", "refs/tags/v1"]
        assert rule.covers("refs/heads/seer/fix")
        assert rule.covers("refs/tags/v1")
        assert not rule.covers("refs/heads/seer/fix2")
        assert not rule.covers("refs/heads/v1")
        assert not rule.covers("refs/tags/seer/fix")

    @pytest.mark.parametrize("ref", ["refs/pull/1/head", "refs/notes/commits", "refs/heads", "refs/tags", "HEAD"])
    def test_patterns_cover_only_branches_and_tags(self, ref):
        rule = PushRule(repository=5, branch_patterns=[".*"], tag_patterns=[".*"])
        assert rule.refs == []
        assert not rule.covers(ref)

    @pytest.mark.parametrize("make_rule", [FetchRule, PushRule], ids=["fetch", "push"])
    def test_same_for_fetch_and_push(self, make_rule):
        rule = make_rule(repository=5, branches=["main"], tag_patterns=["v.+"])
        assert rule.refs == ["refs/heads/main"]
        assert rule.covers("refs/heads/main")
        assert rule.covers("refs/tags/v1")
        assert not rule.covers("refs/heads/dev")

    def test_any_object_covers_every_ref(self):
        rule = FetchRule(repository=5, any_object=True)
        assert rule.refs == []
        assert rule.covers("refs/heads/main")
        assert rule.covers("refs/pull/1/head")

    @pytest.mark.parametrize(
        "pattern,ref,covered",
        [
            (r"v\d+", "refs/tags/v12", True),
            (r"v\d+", "refs/tags/v１２", False),
            (r"seer/\w+", "refs/heads/seer/fix_1", True),
            (r"seer/\w+", "refs/heads/seer/fïx", False),
            # A character the pattern names itself still matches.
            ("ünicode/.+", "refs/heads/ünicode/x", True),
            (r"(?i)seer/.+", "refs/heads/SEER/x", True),
        ],
    )
    def test_patterns_run_with_ascii_classes(self, pattern, ref, covered):
        field = "tag_patterns" if ref.startswith("refs/tags/") else "branch_patterns"
        assert PushRule(repository=5, **{field: [pattern]}).covers(ref) is covered

    def test_pattern_that_asks_for_unicode_classes_cannot_be_built(self):
        with pytest.raises(ValueError, match="is not a regular expression"):
            PushRule(repository=5, tag_patterns=[r"(?u)v\d+"])

    @pytest.mark.parametrize("field", ["branch_patterns", "tag_patterns"])
    def test_cannot_be_built_with_an_invalid_pattern(self, field):
        with pytest.raises(ValueError, match="'seer/\\(' is not a regular expression"):
            PushRule(repository=5, **{field: ["seer/("]})


NAME_FIELDS = pytest.mark.parametrize(
    "field,error", [("branches", "is not a branch name"), ("tags", "is not a tag name")], ids=["branch", "tag"]
)


class TestRefNames:
    @NAME_FIELDS
    @pytest.mark.parametrize("name", INVALID_REF_NAMES)
    def test_invalid(self, field, error, name):
        with pytest.raises(ValueError, match=error):
            PushRule(repository=5, **{field: ["main", name]})
        assert_invalid(
            sign_claims(rule_claims({"operation": "push", "repository": 5, field: ["main", name]})),
            error,
        )

    @NAME_FIELDS
    def test_checked_in_fetch_rules_too(self, field, error):
        with pytest.raises(ValueError, match=error):
            FetchRule(repository=5, **{field: ["refs/heads/main"]})

    def test_tag_named_without_refs_tags(self):
        with pytest.raises(ValueError, match="Name a tag as git tag takes it, without refs/tags/"):
            PushRule(repository=5, tags=["refs/tags/v1"])

    @pytest.mark.parametrize("field", ["branches", "tags"])
    @pytest.mark.parametrize(
        "name", ["main", "seer/fix-123", "feature/a.b", "a@b", "release-1.0", "a.lock.b", "x/HEAD", "ünicode", "v1.0.0"]
    )
    def test_valid(self, field, name):
        assert getattr(PushRule(repository=5, **{field: [name]}), field) == [name]
