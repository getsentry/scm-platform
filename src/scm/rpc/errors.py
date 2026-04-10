from typing import Any

import msgspec

from scm.errors import ErrorCode, SCMCodedError, SCMError
from scm.rpc.types import Error, ErrorResponse

STATUS_MAP: dict[ErrorCode, int] = {
    "malformed_external_id": 400,
    "provider_not_found": 404,
    "rate_limit_exceeded": 429,
    "repository_inactive": 404,
    "repository_not_found": 404,
    "repository_organization_mismatch": 404,
    "rpc_invalid_grant": 401,
    "rpc_malformed_request_body": 400,
    "rpc_malformed_request_headers": 400,
}


class SCMRpcError(SCMError):
    def __init__(
        self,
        code: str | None = None,
        detail: str | None = None,
        meta: dict[str, Any] | None = None,
        status: int | None = None,
        title: str | None = None,
    ):
        self.status = status
        self.code = code
        self.title = title
        self.detail = detail
        self.meta = meta


def map_coded_error(error: SCMCodedError) -> SCMRpcError:
    return SCMRpcError(
        code=error.code,
        title=error.message,
        status=STATUS_MAP[error.code],
    )


def deserialize_rpc_error(error: bytes) -> None:
    """Deserialize an RPC error to an exception type and raise."""

    def map_error(e: Error) -> SCMRpcError:
        status = int(e.status) if e.status else None
        return SCMRpcError(code=e.code, detail=e.detail, meta=e.meta, status=status, title=e.title)

    response = msgspec.json.decode(error, type=ErrorResponse)

    if len(response.errors) == 1:
        raise map_error(response.errors[0])
    else:
        raise ExceptionGroup(
            "Several exceptions were raise while processing your request.",
            [map_error(e) for e in response.errors],
        )


def serialize_rpc_error(exc: SCMRpcError) -> tuple[int, bytes]:
    """Return a tuple of HTTP status code and serialized error data."""
    return (
        exc.status,
        msgspec.json.encode(
            ErrorResponse(
                errors=[
                    Error(
                        code=exc.code,
                        detail=exc.detail,
                        meta=exc.meta,
                        status=str(exc.status),
                        title=exc.title,
                    )
                ]
            )
        ),
    )
