import json
import pathlib
import unittest.mock
from typing import Any

import pytest
import requests

from scm.errors import ResourceBadRequest, ResourceConflict, ResourceNotFound, StaleBranchHead
from scm.providers.bitbucket.provider import (
    BitbucketProvider,
    map_app_installation,
    map_paginated_action,
    map_repository,
    next_cursor,
    parse_scopes,
)
from scm.types import (
    ChmodCommitAction,
    CreateCommitProtocol,
    DeleteCommitAction,
    GetAppInstallationProtocol,
    GetRepositoryProtocol,
    MoveCommitAction,
    Repository,
    WriteCommitAction,
)

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "bitbucket"
REPO = "test-workspace/test-repo"
PARENT = "a682892aa0f4502ef7a242f75d72d0b41eaa27b9"

SUPPORTED_PROTOCOLS = [
    CreateCommitProtocol,
    GetAppInstallationProtocol,
    GetRepositoryProtocol,
]


def recorded(name: str) -> requests.Response:
    """Build a response from a fixture recorded against Bitbucket Cloud."""
    fixture = json.loads((FIXTURES / f"{name}.json").read_text())
    response = requests.Response()
    response.status_code = fixture["status"]
    response.headers.update(fixture["headers"])
    body = fixture["body"]
    response._content = (json.dumps(body) if not isinstance(body, str) else body).encode()
    response.request = requests.Request("GET", "https://api.bitbucket.org/2.0/x").prepare()
    return response


@pytest.fixture
def client() -> unittest.mock.MagicMock:
    return unittest.mock.MagicMock(_name="client")


@pytest.fixture
def provider(client: unittest.mock.MagicMock) -> BitbucketProvider:
    return BitbucketProvider(
        client=client,
        organization_id=1,
        repository=Repository(
            id=1,
            integration_id=1,
            name=REPO,
            organization_id=1,
            is_active=True,
            external_id="{00000000-0000-0000-0000-00000000000a}",
            provider_name="bitbucket",
            web_base_url=None,
            installation_id=None,
        ),
    )


def request_kwargs(client: unittest.mock.MagicMock) -> dict[str, Any]:
    return client.request.call_args.kwargs


@pytest.mark.parametrize("protocol", SUPPORTED_PROTOCOLS, ids=lambda p: p.__name__)
def test_implements_protocol(provider: BitbucketProvider, protocol: type) -> None:
    assert isinstance(provider, protocol)


class TestRequests:
    def test_error_status_raises_coded_error(
        self, provider: BitbucketProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = recorded("repository_not_found")
        with pytest.raises(ResourceNotFound):
            provider.get_repository()

    def test_first_page_sends_no_page(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("branches_page_1")
        provider.get("/2.0/x", pagination={"cursor": "1", "per_page": 2})
        assert request_kwargs(client)["params"] == {"pagelen": "2"}

    def test_later_page_sends_cursor(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("branches_page_1")
        provider.get("/2.0/x", pagination={"cursor": "2", "per_page": 50})
        assert request_kwargs(client)["params"] == {"pagelen": "50", "page": "2"}

    def test_page_size_is_capped(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("branches_page_1")
        provider.get("/2.0/x", pagination={"per_page": 500})
        assert request_kwargs(client)["params"] == {"pagelen": "100"}


class TestPagination:
    def test_next_cursor_from_recorded_page(self) -> None:
        result = map_paginated_action(recorded("branches_page_1"), lambda raw: raw["name"])
        assert len(result["data"]) == 2
        assert result["meta"]["next_cursor"] == "2"

    def test_opaque_page_token(self) -> None:
        raw = {
            "next": "https://api.bitbucket.org/2.0/repositories/test-workspace/test-repo/src/abc/?page=eyJsYXN0IjoiYSJ9"
        }
        assert next_cursor(raw) == "eyJsYXN0IjoiYSJ9"

    def test_last_page(self) -> None:
        assert next_cursor({"values": []}) is None


class TestGetRepository:
    def test_maps_repository(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("repository")

        result = provider.get_repository()

        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}"
        assert result["type"] == "bitbucket"
        assert result["data"] == {
            "full_name": REPO,
            "default_branch": "main",
            "clone_url": f"https://bitbucket.org/{REPO}.git",
            "private": True,
            "size": 2,
            "description": None,
            "topics": [],
        }

    def test_empty_repository_has_no_default_branch(self) -> None:
        raw = json.loads((FIXTURES / "repository.json").read_text())["body"]
        raw["mainbranch"] = None

        assert map_repository(raw)["default_branch"] == ""


class TestGetAppInstallation:
    def test_reads_scopes_from_recorded_header(
        self, provider: BitbucketProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = recorded("repository")

        result = provider.get_app_installation()

        assert result["data"] == {
            "has_read_access": True,
            "has_write_access": True,
            "has_check_run_write_access": True,
        }

    @pytest.mark.parametrize(
        ("header", "read", "write"),
        [
            ("", False, False),
            ("account, repository, pullrequest", True, False),
            ("repository:write, pullrequest", True, False),
            ("repository:write, pullrequest:write", True, True),
            ("read:repository:bitbucket, read:pullrequest:bitbucket", True, False),
            ("write:repository:bitbucket, write:pullrequest:bitbucket", True, True),
        ],
    )
    def test_scope_mapping(self, header: str, read: bool, write: bool) -> None:
        installation = map_app_installation(parse_scopes(header))
        assert installation["has_read_access"] is read
        assert installation["has_write_access"] is write


class TestCreateCommit:
    def test_commits_writes_and_deletes(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [recorded("branch"), recorded("commit_created")]

        result = provider.create_commit(
            "s6-fixture",
            PARENT,
            "Add notes",
            [
                WriteCommitAction(action="create", filename="notes.txt", content="hello\n", encoding="utf-8"),
                DeleteCommitAction(filename="PULL_REQUEST_TEMPLATE.md"),
            ],
            author={"name": "Test User", "email": "test@example.com"},
        )

        branch_call, commit_call = client.request.call_args_list
        assert branch_call.kwargs["path"] == f"/2.0/repositories/{REPO}/refs/branches/s6-fixture"
        assert commit_call.kwargs["method"] == "POST"
        assert commit_call.kwargs["path"] == f"/2.0/repositories/{REPO}/src"
        assert commit_call.kwargs["json"] is False
        assert commit_call.kwargs["data"] == {
            "message": "Add notes",
            "branch": "s6-fixture",
            "parents": PARENT,
            "author": "Test User <test@example.com>",
            "/notes.txt": "hello\n",
            "files": ["PULL_REQUEST_TEMPLATE.md"],
        }
        assert result["data"] == {
            "id": "25a878f7635147b9eef09e958ca34395bc67b705",
            "message": "Add notes",
            "author": {"name": "Test User", "email": "test@example.com", "date": None},
            "additions": None,
            "deletions": None,
        }

    def test_creates_branch(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [recorded("branch_not_found"), recorded("commit_created")]

        result = provider.create_commit("s6-fixture", PARENT, "m", [], create_branch=True)

        assert result["data"]["id"] == "25a878f7635147b9eef09e958ca34395bc67b705"
        assert result["data"]["author"] is None
        assert "author" not in request_kwargs(client)["data"]

    def test_create_branch_conflicts_with_existing_branch(
        self, provider: BitbucketProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = recorded("branch")
        with pytest.raises(ResourceConflict):
            provider.create_commit("s6-fixture", PARENT, "m", [], create_branch=True)
        assert client.request.call_count == 1

    def test_missing_branch_is_not_created(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("branch_not_found")
        with pytest.raises(ResourceNotFound):
            provider.create_commit("s6-no-such-branch", PARENT, "m", [])
        assert client.request.call_count == 1

    def test_moved_head_raises_stale_branch_head(
        self, provider: BitbucketProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.side_effect = [recorded("branch"), recorded("commit_stale_parent")]
        with pytest.raises(StaleBranchHead):
            provider.create_commit("s6-fixture", PARENT, "m", [], expected_head_sha=PARENT)

    def test_moves_text_file(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [
            recorded("branch"),
            recorded("src_meta"),
            recorded("src_file"),
            recorded("commit_created"),
        ]

        provider.create_commit(
            "s6-fixture", PARENT, "m", [MoveCommitAction(old_filename="README.md", new_filename="docs/README.md")]
        )

        meta_call = client.request.call_args_list[1]
        assert meta_call.kwargs["path"] == f"/2.0/repositories/{REPO}/src/{PARENT}/README.md"
        assert meta_call.kwargs["params"] == {"format": "meta"}
        data = request_kwargs(client)["data"]
        assert data["files"] == ["README.md"]
        assert data["/docs/README.md"] == "# test-repo\n\nA test repository.\n"

    def test_refuses_to_move_executable(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [recorded("branch"), recorded("src_meta_executable")]
        with pytest.raises(ResourceBadRequest):
            provider.create_commit(
                "s6-fixture", PARENT, "m", [MoveCommitAction(old_filename="s6-run.sh", new_filename="run.sh")]
            )
        assert client.request.call_count == 2

    @pytest.mark.parametrize(
        "action",
        [
            WriteCommitAction(action="create", filename="logo.png", content="iVBORw0K", encoding="base64"),
            ChmodCommitAction(executable=True, filename="run.sh"),
        ],
        ids=["base64", "chmod"],
    )
    def test_refuses_what_a_text_upload_cannot_write(
        self,
        provider: BitbucketProvider,
        client: unittest.mock.MagicMock,
        action: WriteCommitAction | ChmodCommitAction,
    ) -> None:
        client.request.return_value = recorded("branch")
        with pytest.raises(ResourceBadRequest):
            provider.create_commit("s6-fixture", PARENT, "m", [action])
        assert client.request.call_count == 1

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"force": True},
            {"expected_head_sha": "0" * 40},
            {"expected_head_sha": PARENT, "create_branch": True},
        ],
        ids=["force", "lease_not_parent", "lease_with_create_branch"],
    )
    def test_rejects_unsupported_options(
        self, provider: BitbucketProvider, client: unittest.mock.MagicMock, kwargs: dict[str, Any]
    ) -> None:
        with pytest.raises(ResourceBadRequest):
            provider.create_commit("s6-fixture", PARENT, "m", [], **kwargs)
        client.request.assert_not_called()
