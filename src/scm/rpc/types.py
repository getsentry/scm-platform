from collections.abc import Iterator, Mapping
from typing import Any, Literal

import msgspec

from scm.errors import ErrorCode
from scm.types import CredentialsSet, ProviderName


class ActionAttributes(msgspec.Struct):
    method: str
    path: str
    headers: dict[str, str] | None
    data: dict[str, Any] | None
    params: dict[str, Any] | None
    allow_redirects: bool | None
    stream: bool | None
    credentials_set: CredentialsSet = "installation"


class ActionRequest(msgspec.Struct):
    type: Literal["action"]
    data: ActionAttributes


class RepositoryAttributes(msgspec.Struct):
    external_id: str | None
    integration_id: int
    is_active: bool
    name: str
    organization_id: int
    provider_name: ProviderName


class JsonApiData[Type, Attributes](msgspec.Struct):
    id: str
    type: Type
    attributes: Attributes


class JsonApiPayload[Type, Attributes](msgspec.Struct):
    data: JsonApiData[Type, Attributes]


class Response:
    def __init__(self, status_code: int, headers: Mapping[str, str], content: bytes) -> None:
        self.status_code = status_code
        self.headers = headers
        self.content = content


class StreamResponse:
    def __init__(self, status_code: int, headers: Mapping[str, str], content: Iterator[bytes]) -> None:
        self.status_code = status_code
        self.headers = headers
        self.content = content


class Error(msgspec.Struct):
    code: ErrorCode
    status: str | None = None
    title: str | None = None
    detail: str | None = None
    meta: dict[str, Any] | None = None


class ErrorResponse(msgspec.Struct):
    errors: list[Error]
