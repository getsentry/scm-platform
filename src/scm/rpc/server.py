from collections.abc import Callable, Iterator, Mapping

import msgspec
import requests

from scm.errors import SCMCodedError
from scm.helpers import exec_provider_fn
from scm.manager import SourceCodeManager
from scm.rpc.errors import serialize_error
from scm.rpc.helpers import verify_get, verify_post
from scm.rpc.types import ActionRequest, RepositoryAttributes, RepositoryResponse, Response, StreamResponse
from scm.types import Provider, Repository, RepositoryId


class RpcServer:
    def __init__(
        self,
        secrets: list[str],
        fetch_repository: Callable[[int, RepositoryId], Repository | None],
        fetch_provider: Callable[[int, Repository], Provider | None],
        record_count: Callable[[str, int, dict[str, str]], None],
    ) -> None:
        self.secrets = secrets
        self.fetch_repository = fetch_repository
        self.fetch_provider = fetch_provider
        self.record_count = record_count

    def get(self, headers: dict[str, str]):
        try:
            authorization, organization_id, repository_id = self._extract_headers(headers)

            if not verify_get(self.secrets, organization_id, repository_id, authorization):
                raise SCMCodedError(code="rpc_invalid_grant")

            scm = SourceCodeManager.make_from_repository_id(
                organization_id,
                repository_id,
                referrer=headers.get("X-Referrer", "shared"),
                fetch_repository=self.fetch_repository,
                fetch_provider=self.fetch_provider,
                record_count=self.record_count,
            )

            return Response(status_code=200, headers={}, content=serialize_repository(scm.provider.repository))
        except SCMCodedError as e:
            status, error_data = serialize_error(e)
            return Response(status_code=status, headers={}, content=error_data)

    def post(self, data: bytes, headers: dict[str, str]) -> StreamResponse:
        try:
            return self._post(data, headers)
        except SCMCodedError as e:
            status, error_data = serialize_error(e)
            return StreamResponse(status_code=status, headers={}, content=iter([error_data]))

    def _post(self, data: bytes, headers: dict[str, str]) -> StreamResponse:
        authorization, organization_id, repository_id = self._extract_headers(headers)

        if not verify_post(self.secrets, data, authorization):
            raise SCMCodedError(code="rpc_invalid_grant")

        try:
            action_request = msgspec.json.decode(data, type=ActionRequest)
        except msgspec.DecodeError as e:
            raise SCMCodedError(code="rpc_malformed_request_body") from e

        scm = SourceCodeManager.make_from_repository_id(
            organization_id,
            repository_id,
            referrer=headers.get("X-Referrer", "shared"),
            fetch_repository=self.fetch_repository,
            fetch_provider=self.fetch_provider,
            record_count=self.record_count,
        )

        action = action_request.data
        response = exec_provider_fn(
            scm.provider,
            provider_fn=lambda: scm.provider._request(
                method=action.method,
                path=action.path,
                headers=action.headers,
                data=action.data,
                params=action.params,
                allow_redirects=action.allow_redirects,
                raw_response=True,
            ),
            referrer=scm.referrer,
            record_count=scm.record_count,
        )
        return StreamResponse(
            status_code=response.status_code,
            headers=dict(response.headers),
            content=iter_response(response),
        )

    def _extract_headers(self, headers: Mapping[str, str]) -> tuple[str, int, RepositoryId]:
        try:
            return (
                headers["Authorization"].removeprefix("rpcsignature "),
                int(headers["X-Organization-Id"]),
                msgspec.json.decode(headers["X-Repository-Id"], type=RepositoryId),
            )
        except (KeyError, TypeError, ValueError, msgspec.DecodeError) as e:
            raise SCMCodedError(code="rpc_malformed_request_headers") from e


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
            ),
            type="repository",
        )
    )


def iter_response(response: requests.Response) -> Iterator[bytes]:
    with response as r:
        for chunk in r.iter_content(chunk_size=4096):
            if chunk:
                yield chunk
