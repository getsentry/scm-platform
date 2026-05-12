from typing import cast

import pytest

from scm.errors import SCMError
from scm.helpers import iter_all_pages
from scm.types import PaginatedActionResult, PaginationParams


def _make_page(data: list[int], next_cursor: str | None) -> PaginatedActionResult[list[int]]:
    return cast(
        PaginatedActionResult[list[int]],
        {
            "data": data,
            "type": "github",
            "raw": {},
            "meta": {"next_cursor": next_cursor},
        },
    )


def _action(pages: list[PaginatedActionResult[list[int]]], calls: list[PaginationParams]):
    def fn(params: PaginationParams) -> PaginatedActionResult[list[int]]:
        calls.append(params)
        return pages[len(calls) - 1]

    return fn


def test_iter_all_pages_stops_at_max_pages() -> None:
    # Every page has a non-None next_cursor, so the max_pages guard is the only thing that stops iteration.
    pages = [_make_page([i], str(i + 1)) for i in range(20)]
    calls: list[PaginationParams] = []

    results = list(iter_all_pages(_action(pages, calls), max_pages=3))

    assert len(results) == 3
    assert [r["data"] for r in results] == [[0], [1], [2]]
    # Guard is checked before fetch, so only 3 fetches happen.
    assert len(calls) == 3


def test_iter_all_pages_default_max_pages_is_10() -> None:
    pages = [_make_page([i], str(i + 1)) for i in range(20)]
    calls: list[PaginationParams] = []

    results = list(iter_all_pages(_action(pages, calls)))

    assert len(results) == 10


def test_iter_all_pages_completes_within_limit() -> None:
    pages = [
        _make_page([1], "2"),
        _make_page([2], "3"),
        _make_page([3], None),
    ]
    calls: list[PaginationParams] = []

    results = list(iter_all_pages(_action(pages, calls), max_pages=10))

    assert len(results) == 3
    assert [r["data"] for r in results] == [[1], [2], [3]]


def test_iter_all_pages_stops_on_empty_data() -> None:
    pages = [
        _make_page([1], "2"),
        _make_page([], "3"),
        _make_page([3], None),
    ]
    calls: list[PaginationParams] = []

    results = list(iter_all_pages(_action(pages, calls), max_pages=10))

    assert len(results) == 1
    assert results[0]["data"] == [1]
    assert len(calls) == 2


def test_iter_all_pages_advances_cursor() -> None:
    pages = [
        _make_page([1], "cursor-b"),
        _make_page([2], "cursor-c"),
        _make_page([3], None),
    ]
    calls: list[PaginationParams] = []

    list(iter_all_pages(_action(pages, calls), per_page=25, cursor="cursor-a"))

    assert calls == [
        {"per_page": 25, "cursor": "cursor-a"},
        {"per_page": 25, "cursor": "cursor-b"},
        {"per_page": 25, "cursor": "cursor-c"},
    ]


def test_iter_all_pages_raises_when_max_pages_exceeded() -> None:
    pages = [_make_page([i], str(i + 1)) for i in range(20)]
    calls: list[PaginationParams] = []

    collected: list[PaginatedActionResult[list[int]]] = []
    with pytest.raises(SCMError):
        for page in iter_all_pages(_action(pages, calls), max_pages=3, raise_if_max_pages_exceeded=True):
            collected.append(page)

    # Three pages were yielded before the guard tripped on the 4th iteration.
    assert len(collected) == 3
    assert len(calls) == 3


def test_iter_all_pages_does_not_raise_when_finishing_within_limit() -> None:
    # Iteration terminates naturally via next_cursor=None, so the raise flag should not trigger.
    pages = [
        _make_page([1], "2"),
        _make_page([2], None),
    ]
    calls: list[PaginationParams] = []

    results = list(iter_all_pages(_action(pages, calls), max_pages=10, raise_if_max_pages_exceeded=True))

    assert len(results) == 2


def test_iter_all_pages_max_pages_one_yields_single_page() -> None:
    pages = [_make_page([i], str(i + 1)) for i in range(5)]
    calls: list[PaginationParams] = []

    results = list(iter_all_pages(_action(pages, calls), max_pages=1))

    assert len(results) == 1
    assert results[0]["data"] == [0]
    assert len(calls) == 1
