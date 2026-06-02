import msgspec

from scm.errors import ErrorCode, SCMCodedError
from scm.rpc.types import Error, ErrorResponse

SPECIAL_STATUS_MAP: dict[ErrorCode, int] = {
    "provider_not_found": 404,
    "rate_limit_exceeded": 429,
    "repository_inactive": 404,
    "repository_not_found": 404,
    "repository_organization_mismatch": 404,
    "rpc_invalid_grant": 401,
    "rpc_request_too_large": 413,
    "resource_unauthorized": 401,
    "resource_forbidden": 403,
    "resource_not_found": 404,
    "resource_conflict": 409,
    "resource_unprocessable_content": 422,
    "resource_server_error": 500,
    "resource_bad_gateway": 502,
    "resource_service_unavailable": 503,
    "resource_gateway_timeout": 504,
    "truncated_response": 502,
    "unexpected_response_format": 500,
    "unhandled_exception": 500,
}


def deserialize_error(error: bytes) -> None:
    """Deserialize an RPC error to its concrete exception type and raise."""
    response = msgspec.json.decode(error, type=ErrorResponse)

    if len(response.errors) == 1:
        raise SCMCodedError.from_code(response.errors[0].code, detail=response.errors[0].detail)
    else:
        raise ExceptionGroup(
            "Several exceptions were raise while processing your request.",
            [SCMCodedError.from_code(e.code, detail=e.detail) for e in response.errors],
        )


def serialize_error(exc: SCMCodedError) -> tuple[int, bytes]:
    """Return a tuple of HTTP status code and serialized error data."""
    status_code = SPECIAL_STATUS_MAP.get(exc.code, 400)

    return (
        status_code,
        msgspec.json.encode(
            ErrorResponse(
                errors=[
                    Error(
                        code=exc.code,
                        status=str(status_code),
                        title=exc.message,
                        detail=exc.detail,
                    )
                ]
            )
        ),
    )
