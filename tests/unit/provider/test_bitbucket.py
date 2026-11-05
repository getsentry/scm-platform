import json
import pathlib
import unittest.mock
from datetime import UTC, datetime
from typing import Any

import pytest
import requests

from scm.errors import ResourceForbidden, ResourceNotFound, UnexpectedResponseFormat
from scm.providers.bitbucket.provider import (
    BitbucketProvider,
    map_app_installation,
    map_paginated_action,
    map_pull_request_file,
    map_repository,
    next_cursor,
    parse_scopes,
)
from scm.types import (
    CreatePullRequestCommentProtocol,
    DeletePullRequestCommentProtocol,
    GetAppInstallationProtocol,
    GetAuthenticatedActorProtocol,
    GetPullRequestCommentsProtocol,
    GetPullRequestCommitsProtocol,
    GetPullRequestDiffProtocol,
    GetPullRequestFilesProtocol,
    GetPullRequestProtocol,
    GetPullRequestReviewThreadsProtocol,
    GetPullRequestsProtocol,
    GetPullRequestUrlProtocol,
    GetRepositoryProtocol,
    PullRequestState,
    Repository,
)

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "bitbucket"
REPO = "test-workspace/test-repo"

SUPPORTED_PROTOCOLS = [
    CreatePullRequestCommentProtocol,
    DeletePullRequestCommentProtocol,
    GetAppInstallationProtocol,
    GetAuthenticatedActorProtocol,
    GetPullRequestCommentsProtocol,
    GetPullRequestCommitsProtocol,
    GetPullRequestDiffProtocol,
    GetPullRequestFilesProtocol,
    GetPullRequestProtocol,
    GetPullRequestReviewThreadsProtocol,
    GetPullRequestsProtocol,
    GetPullRequestUrlProtocol,
    GetRepositoryProtocol,
]
TEST_USER = {"id": "{00000000-0000-0000-0000-000000000001}", "username": "test-user"}
APP_USER = {
    "type": "app_user",
    "uuid": "{00000000-0000-0000-0000-000000000002}",
    "display_name": "Test App",
    "account_id": "557058:00000000-0000-0000-0000-000000000002",
    "kind": "connect_app",
}


def with_body(response: requests.Response, body: Any) -> requests.Response:
    response._content = json.dumps(body).encode()
    return response


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


def test_get_authenticated_actor(provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
    client.request.return_value = recorded("user")

    result = provider.get_authenticated_actor()

    assert request_kwargs(client)["path"] == "/2.0/user"
    assert result["data"] == TEST_USER


def test_get_authenticated_actor_app_user(provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
    client.request.return_value = with_body(recorded("user"), APP_USER)

    result = provider.get_authenticated_actor()

    assert result["data"] == {"id": APP_USER["uuid"], "username": "Test App"}


def test_get_authenticated_actor_without_uuid(provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
    client.request.return_value = with_body(recorded("user"), {"type": "app_user"})

    with pytest.raises(UnexpectedResponseFormat):
        provider.get_authenticated_actor()


def test_get_authenticated_actor_forbidden(provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
    client.request.return_value = with_body(recorded("user"), {"type": "error"})
    client.request.return_value.status_code = 403

    with pytest.raises(ResourceForbidden):
        provider.get_authenticated_actor()


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


class TestGetPullRequest:
    def test_maps_open_pull_request(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("pull_request")

        result = provider.get_pull_request("1")

        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/pullrequests/1"
        assert result["data"] == {
            "id": "1",
            "internal_id": "1",
            "title": "Test pull request",
            "body": "A test pull request.",
            "state": "open",
            "merged": False,
            "html_url": f"https://bitbucket.org/{REPO}/pull-requests/1",
            "head": {"ref": "test-branch-1", "sha": "c495d0431cfa"},
            "base": {"ref": "main", "sha": "a682892aa0f4"},
            "author": TEST_USER,
        }

    def test_merged_pull_request_is_closed(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("pull_request_merged")

        pull_request = provider.get_pull_request("2")["data"]

        assert pull_request["state"] == "closed"
        assert pull_request["merged"] is True
        assert pull_request["base"]["ref"] == "s7-merge-base"

    def test_not_found(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("pull_request_not_found")
        with pytest.raises(ResourceNotFound):
            provider.get_pull_request("999")


class TestGetPullRequests:
    def test_maps_page(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("pull_requests_page_1")

        result = provider.get_pull_requests(state=None, pagination={"per_page": 2})

        assert [(pr["id"], pr["state"], pr["merged"]) for pr in result["data"]] == [
            ("3", "closed", False),
            ("2", "closed", True),
        ]
        assert result["data"][0]["body"] is None
        assert result["meta"]["next_cursor"] == "2"

    @pytest.mark.parametrize(
        ("state", "query"),
        [
            ("open", 'state="OPEN"'),
            ("closed", '(state="MERGED" OR state="DECLINED" OR state="SUPERSEDED")'),
            (None, '(state="OPEN" OR state="MERGED" OR state="DECLINED" OR state="SUPERSEDED")'),
        ],
    )
    def test_state_filter(
        self,
        provider: BitbucketProvider,
        client: unittest.mock.MagicMock,
        state: PullRequestState | None,
        query: str,
    ) -> None:
        client.request.return_value = recorded("pull_requests_page_1")
        provider.get_pull_requests(state=state)
        assert request_kwargs(client)["params"] == {"q": query}

    @pytest.mark.parametrize("head", ["feature/x", "test-workspace:feature/x", "test-workspace:refs/heads/feature/x"])
    def test_head_filter(self, provider: BitbucketProvider, client: unittest.mock.MagicMock, head: str) -> None:
        client.request.return_value = recorded("pull_requests_page_1")
        provider.get_pull_requests(head=head)
        assert request_kwargs(client)["params"] == {"q": 'source.branch.name="feature/x" AND state="OPEN"'}

    def test_head_filter_escapes_quotes(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("pull_requests_page_1")
        provider.get_pull_requests(head='a"b')
        assert request_kwargs(client)["params"]["q"].startswith('source.branch.name="a\\"b" AND ')


class TestGetPullRequestFiles:
    def test_maps_diffstat(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("pull_request_diffstat")

        result = provider.get_pull_request_files("2")

        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/pullrequests/2/diffstat"
        assert result["data"] == [
            {
                "filename": "PULL_REQUEST_TEMPLATE.md",
                "status": "removed",
                "patch": None,
                "changes": 3,
                "sha": "",
                "previous_filename": None,
            },
            {
                "filename": "README.md",
                "status": "modified",
                "patch": None,
                "changes": 1,
                "sha": "",
                "previous_filename": None,
            },
            {
                "filename": "src/test_app/greet.py",
                "status": "added",
                "patch": None,
                "changes": 2,
                "sha": "",
                "previous_filename": None,
            },
            {
                "filename": "tests/test_arithmetic.py",
                "status": "renamed",
                "patch": None,
                "changes": 0,
                "sha": "",
                "previous_filename": "tests/test_math.py",
            },
        ]
        assert result["meta"]["next_cursor"] is None

    def test_conflict_status_is_unknown(self) -> None:
        raw = {"status": "merge conflict", "old": None, "new": {"path": "a.py"}, "lines_added": 1, "lines_removed": 0}
        assert map_pull_request_file(raw)["status"] == "unknown"


class TestGetPullRequestCommits:
    def test_maps_commits_oldest_first(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("pull_request_commits")

        result = provider.get_pull_request_commits("2")

        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/pullrequests/2/commits"
        assert result["data"] == [
            {
                "sha": "f1ee80b1c7c743738fb49d063f9a30a880343314",
                "message": "Add greet module\n",
                "author": {
                    "name": "Test User",
                    "email": "test@example.com",
                    "date": datetime(2026, 11, 5, 20, 16, 11, tzinfo=UTC),
                },
            },
            {
                "sha": "a69b43321a2390a5eeff8c4bb711348457a64396",
                "message": "Rename tests, drop template, update README\n",
                "author": {
                    "name": "Test User",
                    "email": "test@example.com",
                    "date": datetime(2026, 11, 5, 20, 16, 11, tzinfo=UTC),
                },
            },
        ]

    @staticmethod
    def with_merge_commit(merge_hash: str) -> requests.Response:
        response = recorded("pull_request_commits")
        body = response.json()
        merge_commit = {
            **body["values"][0],
            "hash": merge_hash,
            "message": "Merged in s7-file-changes (pull request #2)\n",
            "parents": [{"hash": "a682892aa0f4502ef7a242f75d72d0b41eaa27b9"}, {"hash": body["values"][0]["hash"]}],
        }
        body["values"].insert(0, merge_commit)
        response._content = json.dumps(body).encode()
        return response

    def test_drops_bitbucket_merge_commit(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [
            self.with_merge_commit("69c00816acfdb151b90d7dd925cfa79922f362e9"),
            recorded("pull_request_merged"),
        ]

        result = provider.get_pull_request_commits("2")

        assert client.request.call_args_list[1].kwargs["path"] == f"/2.0/repositories/{REPO}/pullrequests/2"
        assert [commit["sha"] for commit in result["data"]] == [
            "f1ee80b1c7c743738fb49d063f9a30a880343314",
            "a69b43321a2390a5eeff8c4bb711348457a64396",
        ]

    def test_keeps_other_merge_commits(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        merge_hash = "1111111111111111111111111111111111111111"
        client.request.side_effect = [self.with_merge_commit(merge_hash), recorded("pull_request_merged")]

        result = provider.get_pull_request_commits("2")

        assert result["data"][-1]["sha"] == merge_hash


class TestGetPullRequestDiff:
    def test_returns_diff_text(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("pull_request_diff")

        result = provider.get_pull_request_diff("1")

        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/pullrequests/1/diff"
        assert request_kwargs(client)["allow_redirects"] is True
        assert result["data"].startswith("diff --git a/src/test_app/math.py b/src/test_app/math.py\n")
        assert "+def subtract(a, b):\n" in result["data"]


def test_get_pull_request_url(provider: BitbucketProvider) -> None:
    assert provider.get_pull_request_url("1") == f"https://bitbucket.org/{REPO}/pull-requests/1"


class TestPullRequestComments:
    def test_lists_general_comments(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("pull_request_comments")

        result = provider.get_pull_request_comments("1")

        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/pullrequests/1/comments"
        assert request_kwargs(client)["params"] == {"q": "inline.path = null AND deleted = false"}
        assert result["data"] == [
            {
                "id": "897197941",
                "body": "Test comment",
                "author": TEST_USER,
                "created_at": "2026-11-05T20:42:57.357805+00:00",
                "author_association": None,
            },
            {
                "id": "897197945",
                "body": "Test reply",
                "author": TEST_USER,
                "created_at": "2026-11-05T20:42:57.876427+00:00",
                "author_association": None,
            },
        ]
        assert result["meta"]["next_cursor"] == "2"

    def test_skips_pending_comments(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        response = recorded("pull_request_comments")
        body = response.json()
        body["values"][0]["pending"] = True
        client.request.return_value = with_body(response, body)

        result = provider.get_pull_request_comments("1")

        assert [comment["id"] for comment in result["data"]] == ["897197945"]

    def test_maps_app_and_missing_authors(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        response = recorded("pull_request_comments")
        body = response.json()
        body["values"][0]["user"] = APP_USER
        del body["values"][1]["user"]
        client.request.return_value = with_body(response, body)

        result = provider.get_pull_request_comments("1")

        assert [comment["author"] for comment in result["data"]] == [
            {"id": APP_USER["uuid"], "username": "Test App"},
            None,
        ]

    def test_create(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("pull_request_comment_created")

        result = provider.create_pull_request_comment("1", "Test created comment")

        assert request_kwargs(client)["method"] == "POST"
        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/pullrequests/1/comments"
        assert request_kwargs(client)["data"] == {"content": {"raw": "Test created comment"}}
        assert result["data"]["id"] == "897199302"
        assert result["data"]["body"] == "Test created comment"
        assert result["data"]["author"] == TEST_USER

    def test_delete(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        response = requests.Response()
        response.status_code = 204
        client.request.return_value = response

        provider.delete_pull_request_comment("1", "897199302")

        assert request_kwargs(client)["method"] == "DELETE"
        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/pullrequests/1/comments/897199302"


class TestGetPullRequestReviewThreads:
    def test_groups_inline_comments(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("pull_request_comments_inline")

        result = provider.get_pull_request_review_threads("1")

        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/pullrequests/1/comments"
        assert request_kwargs(client)["params"] == {
            "q": "inline.path != null",
            "fields": "+values.resolution.type,+values.inline.*",
            "pagelen": "100",
        }
        threads = result["data"]
        assert [
            (t["id"], t["is_resolved"], t["file_path"], t["line"], t["start_line"], [c["body"] for c in t["comments"]])
            for t in threads
        ] == [
            ("897197949", True, "src/test_app/math.py", 3, None, ["Test inline comment", "Test inline reply"]),
            ("897199175", False, "src/test_app/math.py", 4, 3, ["Test multiline comment"]),
            ("897199181", False, "src/test_app/math.py", 2, None, ["Test reply to removed comment"]),
        ]
        assert threads[0]["is_outdated"] is False
        assert threads[0]["comments"][1] == {
            "id": "897197952",
            "unique_id": "897197952",
            "body": "Test inline reply",
            "author": TEST_USER,
            "is_bot": False,
            "created_at": "2026-11-05T20:42:59.180418+00:00",
            "updated_at": "2026-11-05T20:42:59.190806+00:00",
            "is_minimized": False,
            "commit_sha": "c495d0431cfa",
        }
        assert result["meta"]["next_cursor"] is None

    def test_reads_every_page(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        body = recorded("pull_request_comments_inline").json()
        pages = []
        for values, next_url in [
            (body["values"][:3], "https://api.bitbucket.org/2.0/x?page=2"),
            (body["values"][3:], None),
        ]:
            pages.append(
                with_body(recorded("pull_request_comments_inline"), {**body, "values": values, "next": next_url})
            )
        client.request.side_effect = pages

        threads = provider.get_pull_request_review_threads("1")["data"]

        assert client.request.call_args_list[1].kwargs["params"]["page"] == "2"
        assert [c["body"] for c in threads[0]["comments"]] == ["Test inline comment", "Test inline reply"]

    def test_maps_app_users_and_outdated(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        response = recorded("pull_request_comments_inline")
        body = response.json()
        body["values"][0]["user"] = APP_USER
        body["values"][0]["inline"]["outdated"] = True
        del body["values"][1]["user"]
        client.request.return_value = with_body(response, body)

        thread = provider.get_pull_request_review_threads("1")["data"][0]

        assert thread["is_outdated"] is True
        assert [(c["author"], c["is_bot"]) for c in thread["comments"]] == [
            ({"id": APP_USER["uuid"], "username": "Test App"}, True),
            (None, False),
        ]
