import hashlib
import hmac

from scm.types import RepositoryId


def sign_get(secret: str, organization_id: int, repository_id: RepositoryId) -> str:
    return sign(secret, f"{organization_id}{repository_id}".encode())


def sign_post(secret: str, serialized_body: bytes) -> str:
    return sign(secret, serialized_body)


def sign(secret: str, message: bytes) -> str:
    return f"rpc0:{hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()}"


def verify_get(secrets: list[str], organization_id: int, repository_id: RepositoryId, received_signature: str) -> bool:
    return verify(secrets, f"{organization_id}{repository_id}".encode(), received_signature)


def verify_post(secrets: list[str], serialized_body: bytes, received_signature: str) -> bool:
    return verify(secrets, serialized_body, received_signature)


def verify(secrets: list[str], message: bytes, received_signature: str) -> bool:
    return any(hmac.compare_digest(received_signature, sign(secret, message)) for secret in secrets)
