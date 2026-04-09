from collections.abc import Callable, Iterator, Mapping
from typing import Protocol

import msgspec
import requests

from scm.errors import SCMCodedError, SCMError, SCMUnhandledException
from scm.rpc.errors import SCMRpcError, serialize_rpc_error
from scm.rpc.types import ActionRequest, RepositoryAttributes, RepositoryResponse
from scm.types import Provider, Referrer, Repository, RepositoryId


class RpcServerImpl(Protocol):
    def fetch_repository(self, organization_id: int, repository_id: RepositoryId) -> Repository | None: ...
    def initialize_provider(self, organization_id: int, repository: Repository) -> Provider | None: ...
    def record_count(self, name: str, value: int, tags: dict[str, str]) -> None: ...
    def verify_request_signature(self, signature: str, message: bytes) -> bool: ...


class RpcServer:
    def __init__(self, impl: RpcServerImpl) -> None:
        self.impl = impl

    def get(self, headers: Mapping[str, str]):
        authorization = headers["Authorization"]
        organization_id = int(headers["X-Organization-Id"])
        repository_id = msgspec.json.decode(headers["X-Repository-Id"], type=RepositoryId)

    def post(self, data: bytes, headers: Mapping[str, str]) -> requests.Response:
        try:
            authorization = headers["Authorization"]
            organization_id = int(headers["X-Organization-Id"])
            referrer = headers["X-Referrer"]
            repository_id = msgspec.json.decode(headers["X-Repository-Id"], type=RepositoryId)
        except (KeyError, TypeError, ValueError, msgspec.DecodeError) as e:
            raise SCMRpcError(
                code="malformed_request",
                title="Could not deserialize request headers",
                status=400,
                meta={"exception": str(e)},
            ) from e

        if not self.impl.verify_signature(authorization, data):
            raise SCMRpcError(code="invalid_grant", title="Invalid grant", status=401)

        repository = self.impl.fetch_repository(organization_id, repository_id)
        if not repository:
            raise SCMRpcError(
                code="repository_not_found",
                title="The repository could not be found.",
                detail=f"The repository matching '{repository_id}' could not be found.",
                status=404,
            )
        elif not repository["is_active"]:
            raise SCMRpcError(
                code="repository_inactive",
                title="The repository was found but is no longer active.",
                detail=f"The repository matching '{repository_id}' is not active.",
                status=404,
            )
        elif repository["organization_id"] != organization_id:
            raise SCMRpcError(
                code="repository_organization_mismatch",
                title="A repository for another organization was found.",
                status=404,
            )

        provider = self.impl.initialize_provider(organization_id, repository)
        if not provider:
            raise SCMRpcError(
                code="invalid_provider",
                title="No valid service-provider was found.",
                status=404,
            )

        try:
            action_request = msgspec.json.decode(data, type=ActionRequest)
        except msgspec.DecodeError as e:
            raise SCMRpcError(
                code="invalid_request_body",
                title="The request body was invalid.",
                status=400,
            ) from e

        if provider.is_rate_limited(referrer):
            raise SCMRpcError(
                code="rate_limit_exceeded",
                title="The rate-limit was exceeded.",
                status=429,
            )

        return exec_provider_fn(provider, action_request, referrer=referrer, record_count=self.impl.record_count)


def serialize_repository(repository: Repository) -> bytes:
    """Return a serialized repository response type."""
    return msgspec.json.encode(
        RepositoryResponse(
            data=RepositoryAttributes(
                external_id=repository["external_id"],
                integration_id=repository["integration_id"],
                is_active=repository["is_active"],
                name=repository["name"],
                organization_id=repository["organization_id"],
                provider_name=repository["provider_name"],
            )
        )
    )


def exec_provider_fn[T](
    provider: Provider,
    provider_fn: Callable[[], T],
    *,
    referrer: Referrer = "shared",
    record_count: Callable[[str, int, dict[str, str]], None],
) -> T:
    if provider.is_rate_limited(referrer):
        raise SCMCodedError(code="rate_limit_exceeded")

    provider_name = provider.__class__.__name__

    try:
        result = provider_fn()
        record_count("sentry.scm.actions.success_by_provider", 1, {"provider": provider_name})
        record_count("sentry.scm.actions.success_by_referrer", 1, {"referrer": referrer})
        return result
    except SCMError:
        raise
    except Exception as e:
        record_count("sentry.scm.actions.failed_by_provider", 1, {"provider": provider_name})
        record_count("sentry.scm.actions.failed_by_referrer", 1, {"referrer": referrer})
        raise SCMUnhandledException from e


def stream_generator(response: requests.Response) -> Iterator[bytes]:
    with response as r:
        for chunk in r.iter_content(chunk_size=4096):
            if chunk:
                yield chunk
