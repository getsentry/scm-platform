import unittest.mock
from typing import Any

import pytest

from scm.errors import (
    ResourceNotFound,
)
from scm.providers.cursor_origin.provider import (
    CursorOriginProvider,
    map_git_ref,
    map_repository,
)
from scm.types import (
    Repository,
)

REPO = "acme/rocket"


@pytest.fixture
def client() -> unittest.mock.MagicMock:
    return unittest.mock.MagicMock(_name="client")


@pytest.fixture
def provider(client: unittest.mock.MagicMock) -> CursorOriginProvider:
    return CursorOriginProvider(
        client=client,
        organization_id=1,
        repository=Repository(
            id=1,
            integration_id=1,
            name=REPO,
            organization_id=1,
            is_active=True,
            external_id="r_01example",
            provider_name="cursor_origin",
            web_base_url=None,
        ),
        installation_id="inst_01example",
    )


def _response(json_data: Any, status_code: int = 200) -> unittest.mock.MagicMock:
    response = unittest.mock.MagicMock()
    response.json.return_value = json_data
    response.status_code = status_code
    response.headers = {}
    response.content = b"{}"
    return response


REPOSITORY_RAW = {
    "id": "r_01example",
    "name": "rocket",
    "fullName": REPO,
    "owner": {"slug": "acme", "id": "ns_01example", "type": "team"},
    "defaultBranch": "main",
    "cloneUrl": "https://git.cursor.com/acme/rocket.git",
    "visibility": "private",
}


class TestGetRepository:
    def test_it_reads_the_repository(self, provider: CursorOriginProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = _response(REPOSITORY_RAW)

        result = provider.get_repository()

        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}"
        assert client.request.call_args.kwargs["method"] == "GET"
        assert result["type"] == "cursor_origin"
        assert result["data"] == {
            "full_name": REPO,
            "default_branch": "main",
            "clone_url": "https://git.cursor.com/acme/rocket.git",
            "private": True,
            "size": None,
            "description": None,
            "topics": [],
        }

    def test_an_internal_repository_is_private(self) -> None:
        """Visible across the owner but not public, as GitHub reports it."""
        assert map_repository({**REPOSITORY_RAW, "visibility": "internal"})["private"] is True

    def test_a_failure_is_raised_as_a_coded_error(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response({"message": "not found"}, status_code=404)

        with pytest.raises(ResourceNotFound):
            provider.get_repository()


class TestGetBranch:
    def test_a_branch_is_read_as_a_git_ref(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        """Origin has no per-branch route, only the git ref one."""
        client.request.return_value = _response(
            {"ref": "refs/heads/main", "object": {"sha": "9a41f0c3", "type": "commit"}}
        )

        result = provider.get_branch("main")

        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/git/ref/heads/main"
        assert result["data"] == {"ref": "main", "sha": "9a41f0c3"}

    def test_the_refs_prefix_is_stripped(self) -> None:
        raw = {"ref": "refs/heads/release/2.0", "object": {"sha": "abc", "type": "commit"}}

        assert map_git_ref(raw)["ref"] == "release/2.0"


class TestPagination:
    def test_a_cursor_is_sent_as_origins_page_token(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response(REPOSITORY_RAW)

        provider.get(f"/repos/{REPO}", pagination={"cursor": "opaque", "per_page": 50})

        assert client.request.call_args.kwargs["params"] == {
            "pageSize": "50",
            "pageToken": "opaque",
        }

    def test_a_first_page_sends_no_token(self, provider: CursorOriginProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = _response(REPOSITORY_RAW)

        provider.get(f"/repos/{REPO}", pagination={"cursor": "", "per_page": 30})

        assert client.request.call_args.kwargs["params"] == {"pageSize": "30"}

    def test_the_first_page_cursor_sends_no_token(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response(REPOSITORY_RAW)

        provider.get(f"/repos/{REPO}", pagination={"cursor": "1", "per_page": 30})

        assert client.request.call_args.kwargs["params"] == {"pageSize": "30"}
