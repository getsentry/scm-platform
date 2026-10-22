"""Signing and verifying git grants.

A grant travels as an HS256 JWT, the format Sentry and Seer already sign viewer contexts with. The grant's fields
are claims, alongside ``exp``, ``iat`` and an ``aud`` of ``scm-git-grant``. The verifier requires that audience,
so no other token signed with the same secret, such as a viewer context, passes as a grant. A ``kid`` header
names the secret that signed the grant, as in Sentry's viewer contexts, so the verifier can hold several secrets
while they rotate.
"""

import hashlib
import time
from collections.abc import Sequence
from typing import Any

import jwt
import msgspec

from scm.errors import GitGrantExpired, GitInvalidGrant
from scm.git.types import GRANT_MAX_LENGTH, Grant
from scm.secret import Secret, require_secret

GRANT_AUDIENCE = "scm-git-grant"
_ALGORITHM = "HS256"
# Claims the JWT layer owns. Every other claim must be a Grant field.
_JWT_CLAIMS = frozenset({"aud", "exp", "iat"})


def sign_grant(secret: Secret, grant: Grant, *, now: float | None = None) -> str:
    require_secret(secret, parameter="secret")
    # HMAC accepts an empty key, so an empty secret, say from an unset setting, would let anyone sign a grant.
    if not secret:
        raise ValueError("A git grant secret is empty.")
    claims: dict[str, Any] = msgspec.to_builtins(grant)
    claims["exp"] = claims.pop("expires_at")
    claims["iat"] = int(time.time() if now is None else now)
    claims["aud"] = GRANT_AUDIENCE
    token = jwt.encode(claims, secret.reveal(), algorithm=_ALGORITHM, headers={"kid": _key_id(secret)})
    # Refused here, the signer's tests catch a grant that is too long, rather than the sandbox router in production.
    if len(token) > GRANT_MAX_LENGTH:
        raise ValueError(
            f"The git grant is {len(token)} characters, over GRANT_MAX_LENGTH ({GRANT_MAX_LENGTH}). It travels in a "
            "request header, and sandbox-router refuses a longer one."
        )
    return token


def verify_grant(secrets: Sequence[Secret], token: str, *, now: float | None = None) -> Grant:
    """Return the grant if one of ``secrets`` signed it and it has not expired.

    Verification takes a list of secrets, as ``RpcServer`` does, so a new secret can be added before signers
    switch to it and the old one removed after their grants expire.
    """
    for candidate in secrets:
        require_secret(candidate, parameter="secrets")
    # As in sign_grant: a grant signed with an empty secret proves nothing.
    if not all(secrets):
        raise ValueError("A git grant secret is empty.")
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
            secret.reveal(),
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
        # The signature matched, so a holder of the secret wrote these claims; telling them why they are invalid
        # gives nothing away.
        raise GitInvalidGrant(detail=f"The git grant's payload is invalid: {e}") from None

    if grant.expires_at <= now:
        raise GitGrantExpired(detail="The git grant has expired.")
    if not grant.repositories:
        raise GitInvalidGrant(detail="The git grant names no repositories.")
    return grant


def _key_id(secret: Secret) -> str:
    # The first 8 hex characters of the secret's SHA-256, the same key id Sentry's viewer contexts use.
    return hashlib.sha256(secret.reveal().encode()).hexdigest()[:8]
