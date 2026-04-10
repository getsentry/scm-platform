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
    "rpc_malformed_request_body": 400,
    "rpc_malformed_request_headers": 400,
}


def deserialize_error(error: bytes) -> None:
    """Deserialize an RPC error to an exception type and raise."""
    response = msgspec.json.decode(error, type=ErrorResponse)

    if len(response.errors) == 1:
        raise SCMCodedError(code=response.errors[0].code)
    else:
        raise ExceptionGroup(
            "Several exceptions were raise while processing your request.",
            [SCMCodedError(code=e.code) for e in response.errors],
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
                    )
                ]
            )
        ),
    )
