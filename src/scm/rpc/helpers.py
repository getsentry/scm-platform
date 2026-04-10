import hashlib
import hmac

from scm.types import RepositoryId


def sign_get(secret: str, organization_id: int, repository_id: RepositoryId) -> str:
    return sign(secret, f"{organization_id}{repository_id}".encode())


def sign_post(secret: str, serialized_body: bytes) -> str:
    return sign(secret, serialized_body)


def sign(secret: str, message: bytes) -> str:
    return f"rpc0:{hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()}"
