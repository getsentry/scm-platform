from typing import Literal

type ErrorCode = Literal[
    "malformed_external_id",
    "provider_not_found",
    "rate_limit_exceeded",
    "repository_could_not_be_deserialized",
    "repository_inactive",
    "repository_not_found",
    "repository_organization_mismatch",
    "rpc_errors_could_not_be_deserialized",
    "rpc_invalid_grant",
    "rpc_invalid_path",
    "rpc_malformed_request_body",
    "rpc_malformed_request_headers",
    "rpc_request_too_large",
]

ERROR_CODES: dict[ErrorCode, str] = {
    "malformed_external_id": "The repository's external ID was malformed.",
    "provider_not_found": "An unsupported integration provider was found.",
    "rate_limit_exceeded": "Exhausted allocated service-provider quota.",
    "repository_could_not_be_deserialized": "The repository could not be deserialized.",
    "repository_inactive": "A repository was found but it is inactive.",
    "repository_not_found": "A repository could not be found.",
    "repository_organization_mismatch": "A repository was found but it did not belong to your organization.",
    "rpc_errors_could_not_be_deserialized": "The error response could not be deserialized.",
    "rpc_invalid_grant": "Invalid grant",
    "rpc_invalid_path": "The request path was invalid.",
    "rpc_malformed_request_body": "The request body was invalid.",
    "rpc_malformed_request_headers": "The request headers were invalid.",
    "rpc_request_too_large": "The request body exceeded the maximum allowed size.",
}


class SCMError(Exception):
    pass


class SCMCodedError(SCMError):
    def __init__(self, *args, code: ErrorCode, detail: str | None = None, **kwargs) -> None:
        self.code = code
        self.message = ERROR_CODES[code]
        self.detail = detail
        super().__init__(self.code, self.message, *args, **kwargs)


class SCMUnhandledException(SCMError):
    pass


class SCMProviderException(SCMError):
    pass
