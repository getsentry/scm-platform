import unittest.mock
from typing import Any

import pytest

from scm.errors import (
    PathIsDirectory,
    PathIsNotDirectory,
    ResourceBadRequest,
    ResourceNotFound,
    UnexpectedResponseFormat,
)
from scm.providers.cursor_origin.provider import (
    CursorOriginProvider,
    map_file_content,
    map_git_ref,
    map_git_tree,
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
            installation_id="inst_01example",
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


FILE_RAW = {
    "type": "file",
    "encoding": "base64",
    "size": "42",
    "name": "app.py",
    "path": "src/app.py",
    "sha": "b10b5ha",
    "content": "cHJpbnQoImhpIikK",
}

DIRECTORY_RAW = {
    "type": "dir",
    "name": "src",
    "path": "src",
    "sha": "7ree5ha",
    "entries": [
        {"type": "file", "name": "app.py", "path": "src/app.py", "sha": "b10b5ha", "size": "42"},
        {"type": "dir", "name": "web", "path": "src/web", "sha": "d1r5ha", "size": "0"},
    ],
}


class TestGetFileContent:
    def test_a_file_is_read_at_a_ref(self, provider: CursorOriginProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = _response(FILE_RAW)

        result = provider.get_file_content("src/app.py", "main")

        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/contents"
        assert client.request.call_args.kwargs["params"] == {"path": "src/app.py", "ref": "main"}
        assert result["data"]["content"] == "cHJpbnQoImhpIikK"
        assert result["data"]["size"] == 42

    def test_the_size_string_becomes_an_integer(self) -> None:
        """Origin sends 64-bit integers as JSON strings."""
        assert map_file_content(FILE_RAW)["size"] == 42

    def test_a_directory_asked_for_as_a_file_is_refused(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response(DIRECTORY_RAW)

        with pytest.raises(PathIsDirectory):
            provider.get_file_content("src", "main")


class TestGetDirectoryContents:
    def test_the_children_are_listed(self, provider: CursorOriginProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = _response(DIRECTORY_RAW)

        result = provider.get_directory_contents("src", ref="main")

        assert [entry["path"] for entry in result["data"]] == ["src/app.py", "src/web"]
        assert result["data"][1]["type"] == "directory"
        assert result["meta"]["next_cursor"] is None

    def test_a_file_asked_for_as_a_directory_is_refused(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response(FILE_RAW)

        with pytest.raises(PathIsNotDirectory):
            provider.get_directory_contents("src/app.py")


TREE_RAW = {
    "sha": "7ree5ha",
    "tree": [
        {"path": "src", "mode": "040000", "type": "tree", "sha": "d1r5ha"},
        {"path": "src/app.py", "mode": "100644", "type": "blob", "sha": "b10b5ha", "size": 42},
    ],
    "truncated": False,
}


class TestGetTree:
    def test_a_recursive_tree_is_read(self, provider: CursorOriginProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = _response(TREE_RAW)

        result = provider.get_tree("HEAD")

        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/git/trees/HEAD"
        assert client.request.call_args.kwargs["params"] == {"recursive": "true"}
        assert [entry["path"] for entry in result["data"]["tree"]] == ["src", "src/app.py"]
        assert result["data"]["truncated"] is False

    def test_immediate_children_only_when_recursion_is_off(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response(TREE_RAW)

        provider.get_tree("HEAD", recursive=False)

        assert client.request.call_args.kwargs["params"] == {}

    def test_a_tree_entry_has_no_size(self) -> None:
        assert map_git_tree(TREE_RAW)["tree"][0]["size"] is None


def _redirect(location: str) -> unittest.mock.MagicMock:
    response = unittest.mock.MagicMock()
    response.status_code = 302
    response.headers = {"Location": location}
    response.content = b""
    return response


def _streamed_archive() -> unittest.mock.MagicMock:
    response = unittest.mock.MagicMock()
    response.status_code = 200
    response.headers = {}
    response.content = b""
    return response


SIGNED_URL = "https://artifacts.origin.cursor.com/tarballs/abc.tar.gz?Signature=EXAMPLE"


class TestGetArchiveLink:
    def test_a_cached_archive_answers_with_a_signed_url(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _redirect(SIGNED_URL)

        result = provider.get_archive_link("release/test", request_options={"timeout": 600.0})

        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/tarball"
        assert client.request.call_args.kwargs["params"] == {"ref": "release/test"}
        assert client.request.call_args.kwargs["allow_redirects"] is False
        assert client.request.call_args.kwargs["timeout"] == 600.0
        assert client.request.return_value.close.called
        assert result["data"] == {"url": SIGNED_URL, "headers": {}}

    def test_the_first_request_builds_the_archive_and_the_second_links_it(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        """Origin streams the archive inline the first time a commit is asked for."""
        streamed = _streamed_archive()
        client.request.side_effect = [streamed, _redirect(SIGNED_URL)]

        result = provider.get_archive_link("HEAD")

        assert client.request.call_count == 2
        assert streamed.close.called
        assert result["data"]["url"] == SIGNED_URL

    def test_a_response_with_no_location_is_refused(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        response = _redirect(SIGNED_URL)
        response.headers = {}
        client.request.return_value = response

        with pytest.raises(UnexpectedResponseFormat):
            provider.get_archive_link("HEAD")

    def test_only_tarballs_are_offered(self, provider: CursorOriginProvider) -> None:
        """Origin has one archive route, and it is a gzipped tarball."""
        with pytest.raises(ResourceBadRequest):
            provider.get_archive_link("HEAD", archive_format="zip")


class TestDownloadArchive:
    def test_the_archive_is_streamed_with_redirects_followed(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        archive = _streamed_archive()
        client.request.return_value = archive

        response = provider.download_archive("abc123", request_options={"timeout": 600.0})

        assert response is archive
        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/tarball"
        assert client.request.call_args.kwargs["params"] == {"ref": "abc123"}
        assert client.request.call_args.kwargs["allow_redirects"] is None
        assert client.request.call_args.kwargs["timeout"] == 600.0

    def test_only_tarballs_are_offered(self, provider: CursorOriginProvider) -> None:
        with pytest.raises(ResourceBadRequest):
            provider.download_archive("abc123", archive_format="zip")
