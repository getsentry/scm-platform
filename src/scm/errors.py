from typing import Any


class SCMError(Exception):
    pass


class SCMCodedError(SCMError):
    def __init__(self, *args, code: str, **kwargs) -> None:
        self.code = code
        super().__init__(*args, **kwargs)


class SCMUnhandledException(SCMError):
    pass


class SCMProviderException(SCMError):
    pass


class SCMRepositoryCouldNotBeDeserialized(SCMError):
    pass
