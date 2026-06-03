from typing import ClassVar, Literal

type ErrorCode = Literal[
    "malformed_external_id",
    "path_is_directory",
    "path_is_not_directory",
    "provider_not_found",
    "rate_limit_exceeded",
    "readme_not_found",
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
    "resource_bad_request",
    "resource_unauthorized",
    "resource_forbidden",
    "resource_not_found",
    "resource_conflict",
    "resource_unprocessable_content",
    "resource_server_error",
    "resource_bad_gateway",
    "resource_service_unavailable",
    "resource_gateway_timeout",
    "unexpected_response_format",
    "unhandled_exception",
    "draft_pull_request_not_supported",
    "invalid_check_run_state_transition",
]

ERROR_CODES: dict[ErrorCode, str] = {
    "malformed_external_id": "The repository's external ID was malformed.",
    "path_is_directory": "The requested path is a directory, not a file.",
    "path_is_not_directory": "The requested path is not a directory.",
    "provider_not_found": "An unsupported integration provider was found.",
    "rate_limit_exceeded": "Exhausted allocated service-provider quota.",
    "readme_not_found": "The repository does not contain a README file.",
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
    "resource_bad_request": "An error response was received from the service-provider.",
    "resource_unauthorized": "Authentication required. Please provide a valid access token.",
    "resource_forbidden": "You are not authorized to access the requested service-provider resource.",
    "resource_not_found": "The requested resource could not be found on the service-provider.",
    "resource_conflict": "Request could not be processed because it conflicts with the target resource on the server.",
    "resource_unprocessable_content": "Request could not be processed.",
    "resource_server_error": "The service-provider encountered an internal error.",
    "resource_bad_gateway": "The service-provider returned an invalid response from an upstream gateway.",
    "resource_service_unavailable": "The service-provider is temporarily unavailable. The request can be retried.",
    "resource_gateway_timeout": "The service-provider did not respond in time. The request can be retried.",
    "unexpected_response_format": "The response format was in an unexpected format.",
    "unhandled_exception": "An unhandled exception occurred.",
    "draft_pull_request_not_supported": "Draft pull requests are not supported for this repository",
    "invalid_check_run_state_transition": "The requested check run state is not reachable from its current state.",
}


class SCMError(Exception):
    """Base class for every error raised by this library."""


class SCMCodedError(SCMError):
    """An error identified by a stable, serializable :data:`ErrorCode`.

    Each error code has a dedicated subclass (e.g. :class:`RepositoryNotFound`).
    Raising the concrete subclass gives callers first-class exception handling
    (``except RepositoryNotFound:``) and integrates cleanly with Sentry, while
    the underlying ``code`` keeps the error serializable across an RPC boundary.

    Subclasses register themselves by their class-level ``code``. Use
    :meth:`from_code` to reconstruct the appropriate subclass from a wire code.

    The base class may still be instantiated directly with an explicit
    ``code=`` for backwards compatibility, but prefer the concrete subclass.
    """

    code: ErrorCode

    #: Whether the failure is transient and the request may be safely retried with backoff.
    #: Consumers (e.g. Seer) can branch on ``exc.retriable`` instead of enumerating error types.
    retriable: ClassVar[bool] = False

    #: Maps every known error code to its concrete exception subclass.
    _registry: ClassVar[dict[ErrorCode, type["SCMCodedError"]]] = {}

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        code = cls.__dict__.get("code")
        if code is not None:
            SCMCodedError._registry[code] = cls

    def __init__(self, *args, code: ErrorCode | None = None, detail: str | None = None, **kwargs) -> None:
        if code is not None:
            self.code = code
        if not hasattr(self, "code"):
            raise TypeError(f"{type(self).__name__} requires an error 'code'.")

        self.message = detail or ERROR_CODES[self.code]
        self.detail = detail or ERROR_CODES[self.code]
        super().__init__(self.code, self.message, *args, *((k, v) for k, v in kwargs.items()))

    @classmethod
    def from_code(cls, code: ErrorCode, detail: str | None = None) -> "SCMCodedError":
        """Reconstruct the concrete exception for ``code`` (used on the receiving side of an RPC)."""
        error_cls = SCMCodedError._registry.get(code)
        if error_cls is None:
            return SCMCodedError(code=code, detail=detail)
        return error_cls(detail=detail)


class MalformedExternalId(SCMCodedError):
    code = "malformed_external_id"


class PathIsDirectory(SCMCodedError):
    code = "path_is_directory"


class PathIsNotDirectory(SCMCodedError):
    code = "path_is_not_directory"


class ProviderNotFound(SCMCodedError):
    code = "provider_not_found"


class RateLimitExceeded(SCMCodedError):
    code = "rate_limit_exceeded"
    retriable = True


class ReadmeNotFound(SCMCodedError):
    code = "readme_not_found"


class RepositoryCouldNotBeDeserialized(SCMCodedError):
    code = "repository_could_not_be_deserialized"


class RepositoryInactive(SCMCodedError):
    code = "repository_inactive"


class RepositoryNotFound(SCMCodedError):
    code = "repository_not_found"


class RepositoryOrganizationMismatch(SCMCodedError):
    code = "repository_organization_mismatch"


class RpcErrorsCouldNotBeDeserialized(SCMCodedError):
    code = "rpc_errors_could_not_be_deserialized"


class RpcInvalidGrant(SCMCodedError):
    code = "rpc_invalid_grant"


class RpcInvalidPath(SCMCodedError):
    code = "rpc_invalid_path"


class RpcMalformedRequestBody(SCMCodedError):
    code = "rpc_malformed_request_body"


class RpcMalformedRequestHeaders(SCMCodedError):
    code = "rpc_malformed_request_headers"


class RpcRequestTooLarge(SCMCodedError):
    code = "rpc_request_too_large"


class ResourceBadRequest(SCMCodedError):
    code = "resource_bad_request"


class ResourceUnauthorized(SCMCodedError):
    code = "resource_unauthorized"


class ResourceForbidden(SCMCodedError):
    code = "resource_forbidden"


class ResourceNotFound(SCMCodedError):
    code = "resource_not_found"


class ResourceConflict(SCMCodedError):
    code = "resource_conflict"


class ResourceUnprocessableContent(SCMCodedError):
    code = "resource_unprocessable_content"


class ResourceServerError(SCMCodedError):
    code = "resource_server_error"


class ResourceBadGateway(SCMCodedError):
    code = "resource_bad_gateway"


class ResourceServiceUnavailable(SCMCodedError):
    code = "resource_service_unavailable"
    retriable = True


class ResourceGatewayTimeout(SCMCodedError):
    code = "resource_gateway_timeout"
    retriable = True


class UnexpectedResponseFormat(SCMCodedError):
    code = "unexpected_response_format"


class UnhandledException(SCMCodedError):
    code = "unhandled_exception"


class DraftPullRequestNotSupported(SCMCodedError):
    code = "draft_pull_request_not_supported"


class InvalidCheckRunStateTransition(SCMCodedError):
    code = "invalid_check_run_state_transition"


# Maps a service-provider HTTP error status onto a concrete error class.
_STATUS_TO_ERROR: dict[int, type[SCMCodedError]] = {
    400: ResourceBadRequest,
    401: ResourceUnauthorized,
    403: ResourceForbidden,
    404: ResourceNotFound,
    409: ResourceConflict,
    422: ResourceUnprocessableContent,
    429: RateLimitExceeded,
    500: ResourceServerError,
    502: ResourceBadGateway,
    503: ResourceServiceUnavailable,
    504: ResourceGatewayTimeout,
}


def error_class_for_status(status_code: int) -> type[SCMCodedError]:
    """Return the concrete error class modeling a service-provider HTTP error status.

    Unmapped statuses fall back to ``UnhandledException`` so callers can distinguish "the provider
    returned an error we model" from an unexpected response we treat as a defect.
    """
    return _STATUS_TO_ERROR.get(status_code, UnhandledException)
