import datetime
import unittest.mock

import pytest

from scm.errors import MalformedExternalId, ResourceNotFound
from scm.facade import Facade
from scm.providers.perforce.provider import (
    PerforceProvider,
    iter_indexed_records,
    map_action_to_status,
    map_commit,
    map_tree_entry,
    map_tree_entry_mode,
)
from scm.types import ALL_PROTOCOLS, Repository

# Explicit rather than derived, so gaining or losing a capability is a visible diff.
EXPECTED_PROTOCOLS = {
    "DownloadArchiveProtocol",
    "GetAppInstallationProtocol",
    "GetBranchProtocol",
    "GetCommitChangesProtocol",
    "GetCommitProtocol",
    "GetCommitUrlProtocol",
    "GetCommitsByPathProtocol",
    "GetCommitsUrlProtocol",
    "GetFileContentProtocol",
    "GetFileUrlProtocol",
    "GetFullTreeProtocol",
    "GetGitCommitProtocol",
    "GetRepositoryProtocol",
    "GetTreeProtocol",
}

DEPOT = "//SentryDemo/main"


@pytest.fixture
def client():
    return unittest.mock.MagicMock(_name="client")


def _make_provider(client, *, external_id: str | None = DEPOT, web_base_url: str | None = None):
    return PerforceProvider(
        client=client,
        organization_id=1,
        repository=Repository(
            id=1,
            integration_id=1,
            name=DEPOT,
            organization_id=1,
            is_active=True,
            external_id=external_id,
            provider_name="perforce",
            web_base_url=web_base_url,
        ),
    )


@pytest.fixture
def provider(client) -> PerforceProvider:
    return _make_provider(client)


def _response(json_data, status_code: int = 200):
    response = unittest.mock.MagicMock()
    response.json.return_value = json_data
    response.status_code = status_code
    response.headers = {}
    return response


def test_satisfies_exactly_the_read_protocols(provider: PerforceProvider) -> None:
    # Facade derives the method set from structural conformance: a typo'd signature
    # silently drops a capability, a stubbed write silently gains one.
    facade = Facade(provider, record_count=lambda name, value, tags: None)
    assert {p.__name__ for p in ALL_PROTOCOLS if isinstance(facade, p)} == EXPECTED_PROTOCOLS


@pytest.mark.parametrize("external_id", [None, "", "SentryDemo/main"])
def test_rejects_external_id_that_is_not_a_depot_path(client, external_id) -> None:
    # Defaulting to "//..." would silently widen every read to the whole server.
    with pytest.raises(MalformedExternalId):
        _make_provider(client, external_id=external_id)


@pytest.mark.parametrize(
    ("branch", "expected"),
    [(None, f"{DEPOT}/..."), ("release", f"{DEPOT}/release/..."), (f"{DEPOT}/release", f"{DEPOT}/release/...")],
)
def test_depot_scope_resolves_branches(provider: PerforceProvider, branch, expected: str) -> None:
    assert provider.depot_scope(branch) == expected


@pytest.mark.parametrize("branch", ["//OtherDepot/main", "//SentryDemoEvil/main"])
def test_absolute_branch_outside_the_depot_is_refused(provider: PerforceProvider, branch: str) -> None:
    # "//SentryDemoEvil" is the prefix collision a naive startswith would admit.
    with pytest.raises(MalformedExternalId):
        provider.depot_scope(branch)


def test_multi_digit_indices_group_under_the_full_index() -> None:
    # Splitting the index at one digit scrambles every record past the ninth, silently.
    raw = {f"depotFile{i}": f"//depot/f{i}.cpp" for i in range(12)}
    records = iter_indexed_records(raw, "depotFile")
    assert [r["depotFile"] for r in records] == [f"//depot/f{i}.cpp" for i in range(12)]


def test_indexed_fields_without_the_prefix_do_not_invent_a_record() -> None:
    raw = {"change": "1", "otherField0": "x", "depotFile0": "//depot/a.cpp"}
    assert iter_indexed_records(raw, "depotFile") == [{"depotFile": "//depot/a.cpp", "otherField": "x"}]


@pytest.mark.parametrize(
    ("head_type", "expected"),
    [("text", "100644"), ("text+x", "100755"), ("symlink", "120000"), ("binary+FS2w", "100644")],
)
def test_file_type_maps_to_a_git_mode(head_type: str, expected: str) -> None:
    assert map_tree_entry_mode(head_type) == expected


@pytest.mark.parametrize(
    ("action", "expected"),
    [("add", "added"), ("edit", "modified"), ("move/add", "renamed"), ("something-new", "unknown")],
)
def test_action_maps_to_a_file_status(action: str, expected: str) -> None:
    assert map_action_to_status(action) == expected


def test_tree_entry_carries_real_size_and_digest() -> None:
    assert map_tree_entry(
        {"depotFile": f"{DEPOT}/ci.yml", "headType": "text", "fileSize": "1768", "digest": "09A3E1DB"}
    ) == {"path": f"{DEPOT}/ci.yml", "mode": "100644", "type": "blob", "sha": "09A3E1DB", "size": 1768}


def test_tree_entry_without_size_is_none_not_zero() -> None:
    # Consumers test "size is not None" before filtering; 0 would read as an empty file.
    assert map_tree_entry({"depotFile": "//depot/a.cpp", "headType": "text"})["size"] is None


def test_commit_maps_identity_and_prefers_enrichment() -> None:
    assert map_commit({"change": "2993", "user": "someone", "desc": "Import", "time": "1780939216"})["author"] == {
        "name": "someone",
        "email": "someone",
        "date": datetime.datetime(2026, 6, 8, 17, 20, 16, tzinfo=datetime.UTC),
    }
    enriched = map_commit({"change": "1", "user": "someone", "userFullName": "Some One", "userEmail": "s@example.com"})
    assert enriched["author"] == {"name": "Some One", "email": "s@example.com", "date": None}
    assert enriched["author_login"] == "someone"


def test_get_branch_resolves_to_the_newest_changelist(provider: PerforceProvider, client) -> None:
    client.request.return_value = _response([{"change": "2993"}])
    assert provider.get_branch("release")["data"] == {"ref": "release", "sha": "2993"}
    assert client.request.call_args.kwargs["params"] == {"path": f"{DEPOT}/release/...", "max": "1"}


def test_get_full_tree_filters_server_side_and_drops_deletes(provider: PerforceProvider, client) -> None:
    # Listing a deleted revision turns an absence into a spurious "cannot print" error.
    client.request.return_value = _response(
        [
            {"depotFile": f"{DEPOT}/live.cpp", "headType": "text", "headAction": "edit", "fileSize": "10"},
            {"depotFile": f"{DEPOT}/gone.cpp", "headType": "text", "headAction": "delete"},
        ]
    )
    tree = provider.get_full_tree("2993")["data"]

    assert client.request.call_args.kwargs["params"]["filter"] == "headType=text*"
    assert [entry["path"] for entry in tree["tree"]] == [f"{DEPOT}/live.cpp"]


def test_get_tree_never_reports_truncation(provider: PerforceProvider, client) -> None:
    # Truncation sends consumers into a subtree walk that has no Perforce analogue.
    client.request.return_value = _response([])
    assert provider.get_tree("2993")["data"]["truncated"] is False


def test_get_file_content_scopes_the_read_to_a_changelist(provider: PerforceProvider, client) -> None:
    client.request.return_value = _response(
        {
            "stat": {"depotFile": f"{DEPOT}/a.cpp", "type": "text", "fileSize": "5", "digest": "ABC"},
            "content_base64": "aGVsbG8=",
        }
    )
    result = provider.get_file_content("a.cpp", "2993")

    assert client.request.call_args.kwargs["params"] == {"path": f"{DEPOT}/a.cpp@2993"}
    assert result["data"]["content"] == "aGVsbG8="
    assert result["data"]["size"] == 5


def test_get_commit_returns_the_changelist_and_its_files(provider: PerforceProvider, client) -> None:
    client.request.return_value = _response(
        [
            {
                "change": "2993",
                "user": "someone",
                "depotFile0": f"{DEPOT}/a.cpp",
                "action0": "add",
                "depotFile1": f"{DEPOT}/b.cpp",
                "action1": "delete",
            }
        ]
    )
    commit = provider.get_commit("2993")["data"]

    assert commit["id"] == "2993"
    assert [(f["filename"], f["status"]) for f in commit["files"] or []] == [
        (f"{DEPOT}/a.cpp", "added"),
        (f"{DEPOT}/b.cpp", "removed"),
    ]


def test_get_commits_by_path_passes_the_date_window(provider: PerforceProvider, client) -> None:
    client.request.return_value = _response([{"change": "2993", "user": "someone"}])
    result = provider.get_commits_by_path(
        "a.cpp",
        pagination={"per_page": 5, "cursor": "1"},
        since=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        until=datetime.datetime(2026, 2, 1, tzinfo=datetime.UTC),
    )

    assert client.request.call_args.kwargs["params"] == {
        "path": f"{DEPOT}/a.cpp",
        "max": "5",
        "since": "2026-01-01",
        "until": "2026-02-01",
    }
    assert result["meta"]["next_cursor"] is None


def test_get_app_installation_probes_depot_read_access(provider: PerforceProvider, client) -> None:
    # Authenticating to the server is not the same as being allowed the depot.
    client.request.return_value = _response([{"depotFile": f"{DEPOT}/a.cpp"}])
    assert provider.get_app_installation()["data"] == {
        "has_read_access": True,
        "has_write_access": False,
        "has_check_run_write_access": False,
    }


def test_denied_depot_surfaces_as_a_coded_error(provider: PerforceProvider, client) -> None:
    # A protections-table denial must not read as "installed but empty".
    response = _response(None, status_code=404)
    response.content = b"no such file(s)."
    client.request.return_value = response

    with pytest.raises(ResourceNotFound):
        provider.get_app_installation()


def test_download_archive_requests_a_filtered_stream(provider: PerforceProvider, client) -> None:
    client.request.return_value = _response(None)
    provider.download_archive("2993")

    kwargs = client.request.call_args.kwargs
    assert kwargs["params"] == {"path": f"{DEPOT}/...", "change": "2993", "filter": "headType=text*"}
    assert kwargs["stream"] is True


def test_urls_point_at_swarm_when_configured(client) -> None:
    provider = _make_provider(client, web_base_url="https://swarm.example.com/")
    assert (
        provider.get_file_url("Source/a.cpp", "2993", start_line=10, end_line=20)
        == "https://swarm.example.com/files/SentryDemo/main/Source/a.cpp?v=@2993#L10-L20"
    )
    assert provider.get_commit_url("2993") == "https://swarm.example.com/changes/2993"


def test_file_url_without_a_web_ui_is_a_usable_depot_path(provider: PerforceProvider) -> None:

    assert provider.get_file_url("Source/a.cpp", "2993") == f"{DEPOT}/Source/a.cpp@2993"
