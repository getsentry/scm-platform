"""Signing and verifying git grants.

A grant travels as an HS256 JWT, the format Sentry and Seer already sign viewer contexts with. The grant's fields
are claims, alongside ``exp``, ``iat`` and an ``aud`` of ``scm-git-grant``. The verifier requires that audience,
so no other token signed with the same secret, such as a viewer context, passes as a grant. A ``kid`` header
names the secret that signed the grant, as in Sentry's viewer contexts, so the verifier can hold several secrets
while they rotate.
"""

import hashlib
import time
from typing import Any

import jwt
import msgspec

from scm.errors import GitInvalidGrant
from scm.git.types import Grant

GRANT_AUDIENCE = "scm-git-grant"
_ALGORITHM = "HS256"
# Claims the JWT layer owns. Every other claim must be a Grant field.
_JWT_CLAIMS = frozenset({"aud", "exp", "iat"})
# How far in the future a grant's iat may be. Seer signs grants on other hosts than the one verifying them, and a
# clock running a little ahead should not get its grants refused.
_CLOCK_SKEW = 60


def sign_grant(secret: str, grant: Grant, *, now: float | None = None) -> str:
    _check_secrets([secret])
    claims: dict[str, Any] = msgspec.to_builtins(grant)
    claims["exp"] = claims.pop("expires_at")
    claims["iat"] = int(time.time() if now is None else now)
    claims["aud"] = GRANT_AUDIENCE
    return jwt.encode(claims, secret, algorithm=_ALGORITHM, headers={"kid": _key_id(secret)})


def verify_grant(secrets: list[str], token: str, *, now: float | None = None) -> Grant:
    """Return the grant if one of ``secrets`` signed it and it has not expired.

    Verification takes a list of secrets, as ``RpcServer`` does, so a new secret can be added before signers
    switch to it and the old one removed after their grants expire.
    """
    _check_secrets(secrets)
    now = time.time() if now is None else now
    if not token:
        raise GitInvalidGrant(detail="The git grant is missing.")
    try:
        kid = jwt.get_unverified_header(token).get("kid")
    except jwt.DecodeError:
        raise GitInvalidGrant(detail="The git grant is malformed.") from None
    secret = next((secret for secret in secrets if _key_id(secret) == kid), None)
    if secret is None:
        raise GitInvalidGrant(detail="The git grant was not signed with a known secret.")

    try:
        claims = jwt.decode(
            token,
            secret,
            algorithms=[_ALGORITHM],
            audience=GRANT_AUDIENCE,
            # exp and iat are checked below against an injectable clock, so tests can pin it.
            options={"require": sorted(_JWT_CLAIMS), "verify_exp": False, "verify_iat": False},
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
        # The signature matched, so a holder of the secret wrote these claims; telling them why they are invalid
        # gives nothing away.
        raise GitInvalidGrant(detail=f"The git grant's payload is invalid: {e}") from None

    if grant.expires_at <= now:
        raise GitInvalidGrant(detail="The git grant has expired.")
    # bool is an int to isinstance, and PyJWT would refuse it as an iat.
    if not isinstance(claims["iat"], int) or isinstance(claims["iat"], bool):
        raise GitInvalidGrant(detail="The git grant's iat is not an integer.")
    if claims["iat"] > now + _CLOCK_SKEW:
        raise GitInvalidGrant(detail="The git grant was issued in the future.")
    if not grant.repositories:
        raise GitInvalidGrant(detail="The git grant names no repositories.")
    return grant


def _check_secrets(secrets: list[str]) -> None:
    # HMAC accepts an empty key, so an empty secret, say from an unset setting, would let anyone sign a grant.
    if not all(secrets):
        raise ValueError("A git grant secret is empty.")


def _key_id(secret: str) -> str:
    # The first 8 hex characters of the secret's SHA-256, the same key id Sentry's viewer contexts use.
    return hashlib.sha256(secret.encode()).hexdigest()[:8]
