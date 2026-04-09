from typing import Any


class SCMError(Exception):
    pass


class SCMCodedError(SCMError):
    def __init__(self, code: str) -> None:
        self.code = code


class SCMUnhandledException(SCMError):
    pass


class SCMProviderException(SCMError):
    pass


class SCMRepositoryCouldNotBeDeserialized(SCMError):
    pass


class SCMRpcError(SCMError):
    def __init__(
        self,
        code: str | None = None,
        detail: str | None = None,
        meta: dict[str, Any] | None = None,
        status: str | None = None,
        title: str | None = None,
    ):
        self.status = status
        self.code = code
        self.title = title
        self.detail = detail
