from typing import get_args

import msgspec
import pytest

from scm.errors import (
    ERROR_CODES,
    ErrorCode,
    RateLimitExceeded,
    RepositoryNotFound,
    ResourceBadGateway,
    ResourceBadRequest,
    ResourceConflict,
    ResourceForbidden,
    ResourceGatewayTimeout,
    ResourceNotFound,
    ResourceServerError,
    ResourceServiceUnavailable,
    ResourceUnauthorized,
    ResourceUnprocessableContent,
    RpcInvalidGrant,
    SCMCodedError,
    SCMError,
    TruncatedResponse,
    UnhandledException,
    error_class_for_status,
)
from scm.helpers import exec_provider_fn
from scm.rpc.errors import deserialize_error, serialize_error
from scm.rpc.types import Error, ErrorResponse

ALL_CODES: tuple[ErrorCode, ...] = get_args(ErrorCode.__value__)


class TestErrorRegistry:
    def test_every_code_has_a_registered_class(self):
        assert set(SCMCodedError._registry) == set(ALL_CODES)

    def test_registered_class_code_matches_key(self):
        for code, cls in SCMCodedError._registry.items():
            assert cls.code == code

    def test_registered_classes_are_distinct(self):
        classes = list(SCMCodedError._registry.values())
        assert len(classes) == len(set(classes))


class TestConcreteErrors:
    def test_subclass_of_base_types(self):
        exc = RepositoryNotFound()
        assert isinstance(exc, SCMCodedError)
        assert isinstance(exc, SCMError)
        assert isinstance(exc, Exception)

    def test_code_and_message_populated_from_class(self):
        exc = RepositoryNotFound()
        assert exc.code == "repository_not_found"
        assert exc.message == ERROR_CODES["repository_not_found"]
        assert exc.detail == ERROR_CODES["repository_not_found"]

    def test_detail_is_preserved(self):
        exc = ResourceNotFound(detail="pull request org/repo#1")
        assert exc.detail == "pull request org/repo#1"

    def test_positional_args_are_passed_through(self):
        exc = RepositoryNotFound(1, 2)
        assert exc.args[0] == "repository_not_found"
        assert (1, 2) == exc.args[2:]

    def test_can_be_caught_as_concrete_type(self):
        with pytest.raises(RpcInvalidGrant):
            raise RpcInvalidGrant()

    def test_can_be_caught_as_base_type(self):
        with pytest.raises(SCMCodedError):
            raise RpcInvalidGrant()


class TestErrorClassForStatus:
    @pytest.mark.parametrize(
        ("status_code", "expected_type"),
        [
            (400, ResourceBadRequest),
            (401, ResourceUnauthorized),
            (403, ResourceForbidden),
            (404, ResourceNotFound),
            (409, ResourceConflict),
            (422, ResourceUnprocessableContent),
            (429, RateLimitExceeded),
            (500, ResourceServerError),
            (502, ResourceBadGateway),
            (503, ResourceServiceUnavailable),
            (504, ResourceGatewayTimeout),
        ],
    )
    def test_maps_known_status_to_concrete_class(self, status_code, expected_type):
        assert error_class_for_status(status_code) is expected_type

    @pytest.mark.parametrize("status_code", [418, 451, 599])
    def test_unmapped_status_falls_back_to_unhandled_exception(self, status_code):
        assert error_class_for_status(status_code) is UnhandledException


class TestRetriable:
    @pytest.mark.parametrize(
        "error_cls",
        [RateLimitExceeded, ResourceServiceUnavailable, ResourceGatewayTimeout, TruncatedResponse],
    )
    def test_transient_errors_are_retriable(self, error_cls):
        assert error_cls.retriable is True
        assert error_cls().retriable is True

    @pytest.mark.parametrize(
        "error_cls",
        [ResourceNotFound, ResourceBadRequest, ResourceUnprocessableContent, UnhandledException],
    )
    def test_non_transient_errors_are_not_retriable(self, error_cls):
        assert error_cls.retriable is False


class TestFromCode:
    def test_returns_concrete_subclass_for_every_code(self):
        for code in ALL_CODES:
            exc = SCMCodedError.from_code(code)
            assert type(exc) is SCMCodedError._registry[code]
            assert exc.code == code

    def test_preserves_detail(self):
        exc = SCMCodedError.from_code("resource_not_found", detail="boom")
        assert isinstance(exc, ResourceNotFound)
        assert exc.detail == "boom"


class TestBaseClassBackwardsCompatibility:
    def test_base_class_accepts_explicit_code(self):
        exc = SCMCodedError(code="repository_not_found", detail="x")
        assert exc.code == "repository_not_found"
        assert exc.message == "x"
        assert exc.detail == "x"

    def test_base_class_without_code_raises_type_error(self):
        with pytest.raises(TypeError):
            SCMCodedError()


class TestRpcRoundTrip:
    def test_single_error_round_trips_to_concrete_class(self):
        _, payload = serialize_error(RepositoryNotFound(detail="missing"))

        with pytest.raises(RepositoryNotFound) as exc_info:
            deserialize_error(payload)

        assert exc_info.value.code == "repository_not_found"
        assert exc_info.value.detail == "missing"

    def test_every_code_round_trips_to_its_concrete_class(self):
        for code in ALL_CODES:
            _, payload = serialize_error(SCMCodedError.from_code(code, detail="d"))

            with pytest.raises(SCMCodedError) as exc_info:
                deserialize_error(payload)

            assert type(exc_info.value) is SCMCodedError._registry[code]
            assert exc_info.value.code == code
            assert exc_info.value.detail == "d"

    def test_unexpected_exception_detail_survives_the_boundary(self):
        # An unexpected (non-SCM) exception raised while processing is wrapped by exec_provider_fn.
        # The cause chain is local-only, so the type+message must reach the client via `detail`.
        class FakeProvider:
            def is_rate_limited(self, referrer):
                return False

        def boom():
            raise ValueError("connection reset by peer")

        try:
            exec_provider_fn(FakeProvider(), provider_fn=boom, record_count=lambda *a, **k: None)
        except UnhandledException as wrapped:
            _, payload = serialize_error(wrapped)

        with pytest.raises(UnhandledException) as exc_info:
            deserialize_error(payload)

        assert exc_info.value.code == "unhandled_exception"
        assert exc_info.value.detail == "ValueError: connection reset by peer"

    def test_multiple_errors_raise_exception_group_of_concrete_classes(self):
        payload = msgspec.json.encode(
            ErrorResponse(
                errors=[
                    Error(code="repository_not_found", detail="a"),
                    Error(code="rpc_invalid_grant", detail="b"),
                ]
            )
        )

        with pytest.raises(ExceptionGroup) as exc_info:
            deserialize_error(payload)

        exceptions = exc_info.value.exceptions
        assert {type(e) for e in exceptions} == {RepositoryNotFound, RpcInvalidGrant}
        assert {e.detail for e in exceptions} == {"a", "b"}  # type: ignore[attr-defined]
