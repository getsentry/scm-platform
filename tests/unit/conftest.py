from collections.abc import Iterator

import pytest

from scm.git import server


@pytest.fixture(autouse=True)
def _empty_tips_cache() -> Iterator[None]:
    """GitServer caches tips across instances in the process, so one test's tips must not answer another's fetch."""
    server._tips_cache.clear()
    yield
    server._tips_cache.clear()
