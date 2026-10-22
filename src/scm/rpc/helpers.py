import hashlib
import hmac
from collections.abc import Sequence
from typing import Literal

import msgspec

from scm.errors import RepositoryCouldNotBeDeserialized
from scm.rpc.types import JsonApiData, JsonApiPayload, RepositoryAttributes
from scm.secret import Secret, accept_secret, accept_secrets
from scm.types import Repository, RepositoryId

# Each public function wraps its secrets first thing, so a plain string warns once, at the caller's line, and the
# functions it calls only ever see a Secret.


def sign_get(secret: str | Secret, organization_id: int, repository_id: RepositoryId) -> str:
    secret = accept_secret(secret, parameter="secret")
    return sign(secret, f"{organization_id}{repository_id}".encode())


def sign_post(secret: str | Secret, serialized_body: bytes) -> str:
    secret = accept_secret(secret, parameter="secret")
    return sign(secret, serialized_body)


def sign(secret: str | Secret, message: bytes) -> str:
    secret = accept_secret(secret, parameter="secret")
    return f"rpc0:{hmac.new(secret.reveal().encode(), message, hashlib.sha256).hexdigest()}"


def verify_get(
    secrets: Sequence[str | Secret], organization_id: int, repository_id: RepositoryId, received_signature: str
) -> bool:
    secrets = accept_secrets(secrets, parameter="secrets")
    return verify(secrets, f"{organization_id}{repository_id}".encode(), received_signature)


def verify_post(secrets: Sequence[str | Secret], serialized_body: bytes, received_signature: str) -> bool:
    secrets = accept_secrets(secrets, parameter="secrets")
    return verify(secrets, serialized_body, received_signature)


def verify(secrets: Sequence[str | Secret], message: bytes, received_signature: str) -> bool:
    secrets = accept_secrets(secrets, parameter="secrets")
    return any(hmac.compare_digest(received_signature, sign(secret, message)) for secret in secrets)


def deserialize_repository(content: bytes) -> Repository:
    try:
        repository = msgspec.json.decode(content, type=JsonApiPayload[Literal["repository"], RepositoryAttributes]).data
    except msgspec.DecodeError as e:
        raise RepositoryCouldNotBeDeserialized() from e
    else:
        result: Repository = {
            "id": int(repository.id),
            "external_id": repository.attributes.external_id,
            "integration_id": repository.attributes.integration_id,
            "is_active": repository.attributes.is_active,
            "name": repository.attributes.name,
            "organization_id": repository.attributes.organization_id,
            "provider_name": repository.attributes.provider_name,
            "web_base_url": repository.attributes.web_base_url,
            "installation_id": repository.attributes.installation_id,
        }
        return result


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
                    web_base_url=repository["web_base_url"],
                    installation_id=repository["installation_id"],
                ),
            )
        )
    )
