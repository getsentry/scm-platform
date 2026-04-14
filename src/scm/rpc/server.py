from collections.abc import Callable, Iterator, Mapping, MutableMapping

import msgspec
import requests

from scm.errors import ErrorCode, SCMCodedError
from scm.helpers import exec_provider_fn
from scm.manager import SourceCodeManager
from scm.rpc.errors import serialize_error
from scm.rpc.helpers import serialize_repository, verify_get, verify_post
from scm.rpc.types import ActionRequest, Response, StreamResponse
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

            return Response(
                status_code=200,
                headers={"Content-Type": "application/json"},
                content=serialize_repository(scm.provider.repository),
            )
        except SCMCodedError as e:
            status, error_data = serialize_error(e)
            return Response(status_code=status, headers={"Content-Type": "application/json"}, content=error_data)

    def post(self, data: bytes, headers: dict[str, str]) -> StreamResponse:
        try:
            return self._post(data, headers)
        except SCMCodedError as e:
            status, error_data = serialize_error(e)
            return StreamResponse(
                status_code=status,
                headers={"Content-Type": "application/json"},
                content=iter([error_data]),
            )

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
            headers=normalize_headers(response.headers),
            content=iter_response(response),
        )

    def _extract_headers(self, headers: Mapping[str, str]) -> tuple[str, int, RepositoryId]:
        code: ErrorCode = "rpc_malformed_request_headers"

        try:
            authorization = headers["Authorization"].removeprefix("rpcsignature ")
        except KeyError as e:
            raise SCMCodedError(code=code, detail="Could not find Authorization header") from e

        try:
            raw = headers["X-Organization-Id"]
            organization_id = int(raw)
        except KeyError as e:
            raise SCMCodedError(code=code, detail="Could not find X-Organization-Id header") from e
        except ValueError as e:
            raise SCMCodedError(code=code, detail="Could not parse X-Organization-Id header") from e

        try:
            repository_id = msgspec.json.decode(headers["X-Repository-Id"], type=RepositoryId)
        except KeyError as e:
            raise SCMCodedError(code=code, detail="Could not find X-Repository-Id header") from e
        except msgspec.DecodeError as e:
            raise SCMCodedError(code=code, detail="Could not parse X-Repository-Id header") from e

        return authorization, organization_id, repository_id


def iter_response(response: requests.Response) -> Iterator[bytes]:
    with response as r:
        yield from filter(bool, r.iter_content(chunk_size=64 * 1024))


# Transport-level headers that describe upstream wire framing, not the payload
# itself. iter_response() decodes the body so these no longer apply.
_HOP_BY_HOP = frozenset(
    {
        "authorization",
        "connection",
        "content-encoding",
        "content-length",
        "host",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "set-cookie",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)


def normalize_headers(headers: MutableMapping[str, str]) -> dict[str, str]:
    """Remove wire-framing headers and other private headers we do not want to leak."""
    return {k: v for k, v in headers.items() if k.lower() not in _HOP_BY_HOP}
