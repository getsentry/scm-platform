import hashlib
import hmac
from typing import Literal

import msgspec

from scm.errors import SCMCodedError
from scm.rpc.types import JsonApiData, JsonApiPayload, RepositoryAttributes
from scm.types import Repository, RepositoryId


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


def deserialize_repository(content: bytes) -> Repository:
    try:
        repository = msgspec.json.decode(content, type=JsonApiPayload[Literal["repository"], RepositoryAttributes]).data
    except msgspec.DecodeError as e:
        raise SCMCodedError(code="repository_could_not_be_deserialized") from e
    else:
        return {
            "id": int(repository.id),
            "external_id": repository.attributes.external_id,
            "integration_id": repository.attributes.integration_id,
            "is_active": repository.attributes.is_active,
            "name": repository.attributes.name,
            "organization_id": repository.attributes.organization_id,
            "provider_name": repository.attributes.provider_name,
        }


def serialize_repository(repository: Repository) -> bytes:
    """Return a serialized repository response type."""
    return msgspec.json.encode(
        JsonApiPayload(
            data=JsonApiData(
                id=str(repository["id"]),
                type="repository",
                attributes=RepositoryAttributes(
                    external_id=repository["external_id"],
                    integration_id=repository["integration_id"],
                    is_active=repository["is_active"],
                    name=repository["name"],
                    organization_id=repository["organization_id"],
                    provider_name=repository["provider_name"],
                ),
            )
        )
    )
