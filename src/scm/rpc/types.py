from collections.abc import Iterator, Mapping
from typing import Any, Literal

import msgspec

from scm.types import ProviderName


class ActionAttributes(msgspec.Struct):
    method: str
    path: str
    headers: Mapping[str, str] | None
    data: Mapping[str, Any] | None
    params: Mapping[str, str] | None
    allow_redirects: bool


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


class RepositoryResponse(msgspec.Struct):
    type: Literal["repository"]
    data: RepositoryAttributes


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
    status: str | None = None
    code: str | None = None
    title: str | None = None
    detail: str | None = None
    meta: dict[str, Any] | None = None


class ErrorResponse(msgspec.Struct):
    errors: list[Error]
