from collections.abc import Iterator, Mapping
from typing import Protocol

import msgspec
import requests

from scm.helpers import exec_provider_fn
from scm.rpc.errors import SCMRpcError, serialize_rpc_error
from scm.rpc.types import ActionRequest, RepositoryAttributes, RepositoryResponse, Response, StreamResponse
from scm.types import Provider, Repository, RepositoryId


class RpcServerImpl(Protocol):
    def fetch_repository(self, organization_id: int, repository_id: RepositoryId) -> Repository | None: ...
    def initialize_provider(self, organization_id: int, repository: Repository) -> Provider | None: ...
    def record_count(self, name: str, value: int, tags: dict[str, str]) -> None: ...
    def verify_request_signature(self, signature: str, message: bytes) -> bool: ...


class RpcServer:
    def __init__(self, impl: RpcServerImpl) -> None:
        self.impl = impl

    def get(self, headers: Mapping[str, str]):
        try:
            return self._get(headers)
        except SCMRpcError as e:
            status, error_data = serialize_rpc_error(e)
            return Response(status_code=status, headers={}, content=iter([error_data]))

    def post(self, data: bytes, headers: Mapping[str, str]) -> StreamResponse:
        try:
            return self._post(data, headers)
        except SCMRpcError as e:
            status, error_data = serialize_rpc_error(e)
            return StreamResponse(status_code=status, headers={}, content=iter([error_data]))

    def _get(self, headers: Mapping[str, str]):
        authorization, organization_id, repository_id = self._extract_headers(headers)
        self._authorize_request(authorization, f"{headers['X-Organization-Id']}{headers['X-Repository-Id']}")
        repository = self._fetch_valid_repository(organization_id, repository_id)
        return Response(status_code=200, headers={}, content=serialize_repository(repository))

    def _post(self, data: bytes, headers: Mapping[str, str]) -> StreamResponse:
        authorization, organization_id, repository_id = self._extract_headers(headers)
        self._authorize_request(authorization, data)
        repository = self._fetch_valid_repository(organization_id, repository_id)
        provider = self._initialize_provider(organization_id, repository)

        try:
            action_request = msgspec.json.decode(data, type=ActionRequest)
        except msgspec.DecodeError as e:
            raise SCMRpcError(
                code="invalid_request_body",
                title="The request body was invalid.",
                status=400,
            ) from e

        action = action_request.data
        response = exec_provider_fn(
            provider,
            provider_fn=lambda: provider.api_client._request(
                method=action.method,
                path=action.path,
                headers=action.headers,
                data=action.data,
                params=action.params,
                allow_redirects=action.allow_redirects,
                raw_response=True,
            ),
            referrer=headers.get("X-Referrer", "shared"),
            record_count=self.impl.record_count,
        )
        return StreamResponse(
            status_code=response.status_code,
            headers=dict(response.headers),
            content=iter_response(response),
        )

    def _authorize_request(self, signature: str, message: bytes):
        if not self.impl.verify_request_signature(signature, message):
            raise SCMRpcError(code="invalid_grant", title="Invalid grant", status=401)

    def _extract_headers(self, headers: Mapping[str, str]) -> tuple[str, int, RepositoryId]:
        try:
            return (
                headers["Authorization"],
                int(headers["X-Organization-Id"]),
                msgspec.json.decode(headers["X-Repository-Id"], type=RepositoryId),
            )
        except (KeyError, TypeError, ValueError, msgspec.DecodeError) as e:
            raise SCMRpcError(
                code="malformed_request",
                title="Could not deserialize request headers",
                status=400,
                meta={"exception": str(e)},
            ) from e

    def _fetch_valid_repository(self, organization_id: int, repository_id: RepositoryId) -> Repository:
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

        return repository

    def _initialize_provider(self, organization_id: int, repository: Repository) -> Provider:
        provider = self.impl.initialize_provider(organization_id, repository)
        if not provider:
            raise SCMRpcError(
                code="invalid_provider",
                title="No valid service-provider was found.",
                status=404,
            )
        return provider


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


def iter_response(response: requests.Response) -> Iterator[bytes]:
    with response as r:
        for chunk in r.iter_content(chunk_size=4096):
            if chunk:
                yield chunk
