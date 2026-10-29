"""Signing and verifying git grants.

A grant travels as a JWT signed with Ed25519 (``alg`` ``EdDSA``). Seer signs grants with the only private key, and
Sentry verifies them against a list of public keys, so nothing that verifies a grant can mint one, and the list of
public keys is not a secret. The grant's fields are claims, alongside ``exp``, ``iat`` and an ``aud`` of
``scm-git-grant``. The verifier requires that audience, so no other token signed with the same key passes as a
grant. A ``kid`` header names the key that signed the grant, so the verifier can list several public keys while
they rotate.

Keys are PEM strings, as for objectstore's tokens: the private key as PKCS#8 (``-----BEGIN PRIVATE KEY-----``),
public keys as SubjectPublicKeyInfo (``-----BEGIN PUBLIC KEY-----``). ``openssl genpkey -algorithm ed25519`` writes
a private key, and ``openssl pkey -pubout`` its public key.
"""

import hashlib
import time
from collections.abc import Sequence
from typing import Any

import jwt
import msgspec
from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from cryptography.hazmat.primitives.serialization import load_pem_private_key, load_pem_public_key

from scm.errors import GitGrantExpired, GitInvalidGrant
from scm.git.types import GRANT_MAX_LENGTH, Grant
from scm.secret import Secret

GRANT_AUDIENCE = "scm-git-grant"
# The only algorithm the verifier accepts, with parsed Ed25519 keys. A token that names HS256 and uses a public key,
# which is no secret, as its HMAC key is refused, as is one that names ``none``.
_ALGORITHM = "EdDSA"
# Claims the JWT layer owns. Every other claim must be a Grant field.
_JWT_CLAIMS = frozenset({"aud", "exp", "iat"})


def sign_grant(private_key: Secret, grant: Grant, *, now: float | None = None) -> str:
    """Sign ``grant`` with ``private_key``, an Ed25519 private key in PEM.

    The ``kid`` header is derived from the key, so a signer needs no key id setting next to it.
    """
    key = _load_private_key(private_key)
    claims: dict[str, Any] = msgspec.to_builtins(grant)
    claims["exp"] = claims.pop("expires_at")
    claims["iat"] = int(time.time() if now is None else now)
    claims["aud"] = GRANT_AUDIENCE
    # PyJWT gets the parsed key rather than the PEM, so none of its frames holds the key as a string.
    token = jwt.encode(claims, key, algorithm=_ALGORITHM, headers={"kid": _key_id(key.public_key())})
    # Refused here, the signer's tests catch a grant that is too long, rather than the sandbox router in production.
    if len(token) > GRANT_MAX_LENGTH:
        raise ValueError(
            f"The git grant is {len(token)} characters, over GRANT_MAX_LENGTH ({GRANT_MAX_LENGTH}). It travels in a "
            "request header, and sandbox-router refuses a longer one."
        )
    return token


def verify_grant(public_keys: Sequence[str], token: str, *, now: float | None = None) -> Grant:
    """Return the grant if the private key of one of ``public_keys`` signed it and it has not expired.

    Verification takes a list of Ed25519 public keys in PEM, so a new key can be added before the signer switches to
    it and the old one removed after its grants expire. Raises ValueError, rather than refusing the grant, if the
    list is empty or a key is not one, since no grant could verify against it.
    """
    keys = _public_keys_by_id(public_keys)
    now = time.time() if now is None else now
    if not token:
        raise GitInvalidGrant(detail="The git grant is missing.")
    try:
        kid = jwt.get_unverified_header(token).get("kid")
    except jwt.InvalidTokenError:
        # A DecodeError, or a kid that is not a string.
        raise GitInvalidGrant(detail="The git grant is malformed.") from None
    # PyJWT has checked that a kid is a string. A grant without one names no key.
    key = keys.get(kid) if kid is not None else None
    if key is None:
        raise GitInvalidGrant(detail="The git grant was not signed with a known key.")

    try:
        claims = jwt.decode(
            token,
            key,
            algorithms=[_ALGORITHM],
            audience=GRANT_AUDIENCE,
            # exp is checked below against an injectable clock, so tests can pin it. PyJWT checks iat, which it
            # also requires to be a number.
            options={"require": sorted(_JWT_CLAIMS), "verify_exp": False},
            # How far in the future iat may be, in seconds. Seer signs grants on other hosts than the one verifying
            # them, and a clock running a little ahead should not get its grants refused.
            leeway=60,
        )
    except jwt.InvalidSignatureError:
        raise GitInvalidGrant(detail="The git grant's signature does not match.") from None
    except jwt.DecodeError:
        raise GitInvalidGrant(detail="The git grant is malformed.") from None
    except jwt.InvalidTokenError as e:
        # The signature matched or was never reached, and PyJWT's reasons name only claims, not values.
        raise GitInvalidGrant(detail=f"The git grant is invalid: {e}") from None

    fields = {name: value for name, value in claims.items() if name not in _JWT_CLAIMS}
    try:
        grant = msgspec.convert({**fields, "expires_at": claims["exp"]}, type=Grant, strict=True)
    except msgspec.ValidationError as e:
        # The signature matched, so the holder of the private key wrote these claims; telling them why they are
        # invalid gives nothing away.
        raise GitInvalidGrant(detail=f"The git grant's payload is invalid: {e}") from None

    if grant.expires_at <= now:
        raise GitGrantExpired(detail="The git grant has expired.")
    if not grant.repositories:
        raise GitInvalidGrant(detail="The git grant names no repositories.")
    return grant


def require_public_keys(public_keys: Sequence[str]) -> list[str]:
    """Return ``public_keys`` as a list, if there is at least one and each is an Ed25519 public key in PEM.

    For GitServer, which verifies a grant against its keys on every request. A key that does not parse would make
    every request fail, so the server fails once, when it is built.
    """
    _public_keys_by_id(public_keys)
    return list(public_keys)


def _load_private_key(private_key: Secret) -> Ed25519PrivateKey:
    if not private_key:
        raise ValueError("The git grant signing key is empty.")
    try:
        # Revealed inline, so no frame holds the PEM outside its Secret.
        key = load_pem_private_key(private_key.reveal().encode(), password=None)
    except (ValueError, TypeError, UnsupportedAlgorithm) as e:
        # cryptography's reason, chained, names no key material.
        raise ValueError(
            "The git grant signing key is not an unencrypted private key in PEM (-----BEGIN PRIVATE KEY-----)."
        ) from e
    if not isinstance(key, Ed25519PrivateKey):
        raise ValueError(f"The git grant signing key must be an Ed25519 key, not {type(key).__name__}.")
    return key


def _public_keys_by_id(public_keys: Sequence[str]) -> dict[str, Ed25519PublicKey]:
    if not public_keys:
        raise ValueError("There are no git grant public keys, so no grant could verify.")
    keys: dict[str, Ed25519PublicKey] = {}
    for index, pem in enumerate(public_keys):
        key = _load_public_key(pem, index)
        keys[_key_id(key)] = key
    return keys


def _load_public_key(pem: str, index: int) -> Ed25519PublicKey:
    # Each message names the key by its place in the list rather than quoting it.
    if not isinstance(pem, str):
        raise TypeError(f"public_keys[{index}] must be a PEM string, not {type(pem).__name__}.")
    # load_pem_public_key refuses a private key too, but saying what it is tells the operator to move it away from
    # where verifiers read their keys.
    if "PRIVATE KEY" in pem:
        raise ValueError(
            f"public_keys[{index}] is a private key. Verifiers take public keys only; the private key stays with the "
            "signer."
        )
    try:
        key = load_pem_public_key(pem.encode())
    except (ValueError, UnsupportedAlgorithm) as e:
        raise ValueError(f"public_keys[{index}] is not a public key in PEM (-----BEGIN PUBLIC KEY-----).") from e
    if not isinstance(key, Ed25519PublicKey):
        raise ValueError(f"public_keys[{index}] must be an Ed25519 key, not {type(key).__name__}.")
    return key


def _key_id(public_key: Ed25519PublicKey) -> str:
    # The first 16 hex characters of the SHA-256 of the raw public key. Derived from the key, so the signer computes
    # it from its private key and the verifier from each listed public key, and neither needs a kid setting. Not an
    # RFC 7638 thumbprint: the kid only picks a key from the verifier's own list, never one from a published key set,
    # so the standard would buy nothing but length (43 characters) in a grant whose length is capped. 64 bits keep
    # two listed keys from ever sharing an id.
    return hashlib.sha256(public_key.public_bytes_raw()).hexdigest()[:16]
