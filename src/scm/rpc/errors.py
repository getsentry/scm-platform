from typing import Any

import msgspec

from scm.errors import SCMError
from scm.rpc.types import Error, ErrorResponse


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
