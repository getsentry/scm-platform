"""Signing and verifying git grants.

The wire format is ``<base64url(payload)>.<base64url(signature)>``, where the payload is the grant's JSON and the
signature is an HMAC-SHA256 over a fixed prefix and the payload. The prefix keeps a grant signature from ever
matching a signature the RPC endpoint makes with the same secret over some other message.
"""

import base64
import hashlib
import hmac
import re
import time

import msgspec

from scm.errors import GitInvalidGrant
from scm.git.types import Grant

_SIGNATURE_DOMAIN = b"scm-git-grant."
_BASE64URL = re.compile(r"[A-Za-z0-9_-]+")


def sign_grant(secret: str, grant: Grant) -> str:
    payload = msgspec.json.encode(grant)
    return f"{_b64encode(payload)}.{_b64encode(_signature(secret, payload))}"


def verify_grant(secrets: list[str], token: str, *, now: float | None = None) -> Grant:
    """Return the grant if any of ``secrets`` signed it and it has not expired.

    Verification takes a list of secrets, as ``RpcServer`` does, so a new secret can be added before signers
    switch to it and the old one removed after their grants expire.
    """
    if not token:
        raise GitInvalidGrant(detail="The git grant is missing.")
    try:
        payload_b64, signature_b64 = token.split(".")
        payload = _b64decode(payload_b64)
        signature = _b64decode(signature_b64)
    except ValueError:  # binascii.Error is a ValueError too
        raise GitInvalidGrant(detail="The git grant is malformed.") from None

    # Check every secret, not just until one matches, so the time taken does not reveal which one signed it.
    matches = [hmac.compare_digest(signature, _signature(secret, payload)) for secret in secrets]
    if not any(matches):
        raise GitInvalidGrant(detail="The git grant's signature does not match.")

    try:
        grant = msgspec.json.decode(payload, type=Grant)
    except msgspec.ValidationError as e:
        # Only a holder of the secret can get here, so the reason is safe to return and saves a debugging round.
        raise GitInvalidGrant(detail=f"The git grant's payload is invalid: {e}") from None
    except msgspec.DecodeError:
        raise GitInvalidGrant(detail="The git grant's payload is not JSON.") from None

    if grant.expires_at <= (time.time() if now is None else now):
        raise GitInvalidGrant(detail="The git grant has expired.")
    if not grant.repositories:
        raise GitInvalidGrant(detail="The git grant names no repositories.")
    return grant


def _signature(secret: str, payload: bytes) -> bytes:
    return hmac.new(secret.encode(), _SIGNATURE_DOMAIN + payload, hashlib.sha256).digest()


def _b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64decode(data: str) -> bytes:
    # urlsafe_b64decode skips characters outside its alphabet; refusing them keeps one token to one decoding.
    if not _BASE64URL.fullmatch(data):
        raise ValueError("not base64url")
    return base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))
