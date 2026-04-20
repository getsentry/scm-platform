import msgspec

from scm.errors import ErrorCode, SCMCodedError
from scm.rpc.types import Error, ErrorResponse

STATUS_MAP: dict[ErrorCode, int] = {
    "malformed_external_id": 400,
    "provider_not_found": 404,
    "rate_limit_exceeded": 429,
    "repository_inactive": 404,
    "repository_not_found": 404,
    "repository_organization_mismatch": 404,
    "rpc_invalid_grant": 401,
    "rpc_invalid_path": 400,
    "rpc_malformed_request_body": 400,
    "rpc_malformed_request_headers": 400,
    "rpc_request_too_large": 413,
    "resource_bad_request": 400,
    "resource_forbidden": 403,
    "resource_not_found": 404,
    "unexpected_response_format": 500,
    "unhandled_exception": 500,
}


def deserialize_error(error: bytes) -> None:
    """Deserialize an RPC error to an exception type and raise."""
    response = msgspec.json.decode(error, type=ErrorResponse)

    if len(response.errors) == 1:
        raise SCMCodedError(code=response.errors[0].code, detail=response.errors[0].detail)
    else:
        raise ExceptionGroup(
            "Several exceptions were raise while processing your request.",
            [SCMCodedError(code=e.code, detail=response.errors[0].detail) for e in response.errors],
        )


def serialize_error(exc: SCMCodedError) -> tuple[int, bytes]:
    """Return a tuple of HTTP status code and serialized error data."""
    return (
        STATUS_MAP[exc.code],
        msgspec.json.encode(
            ErrorResponse(
                errors=[
                    Error(
                        code=exc.code,
                        status=str(STATUS_MAP[exc.code]),
                        title=exc.message,
                        detail=exc.detail,
                    )
                ]
            )
        ),
    )
