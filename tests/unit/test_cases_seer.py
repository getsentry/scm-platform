from datetime import UTC, datetime

from scm.cases import seer
from scm.types import (
    ActionResult,
    Commit,
    CommitAuthor,
    CommitFile,
    FileContent,
    GitCommitObject,
    GitCommitTree,
    GitTree,
    PaginatedActionResult,
    PaginatedResponseMeta,
    PaginationParams,
    RequestOptions,
    TreeEntry,
)
from tests.test_fixtures import BaseTestProvider, SourceCodeManager


def _build_file_tree(files: list[dict[str, str]]) -> str:
    return "\n".join(f"{f['status']} {f['path']}" for f in files)


def _action(data):
    return ActionResult(data=data, type="github", raw={"headers": None, "data": None}, meta={})


def _paginated(data, next_cursor=None):
    return PaginatedActionResult(
        data=data,
        type="github",
        raw={"headers": None, "data": None},
        meta=PaginatedResponseMeta(next_cursor=next_cursor),
    )


# ---------------------------------------------------------------------------
# get_file_content
# ---------------------------------------------------------------------------


def test_get_file_content_base64_decodes():
    scm = SourceCodeManager(BaseTestProvider())
    # BaseTestProvider returns "SGVsbG8gV29ybGQ=" (base64 "Hello World")
    assert seer.get_file_content(scm, "README.md", "abc123") == b"Hello World"


def test_get_file_content_plain_text_encodes_utf8():
    class Provider(BaseTestProvider):
        def get_file_content(self, path, ref=None, request_options=None):
            return _action(FileContent(path=path, sha="s", content="héllo", encoding="utf-8", size=5))

    scm = SourceCodeManager(Provider())
    assert seer.get_file_content(scm, "README.md", "abc123") == "héllo".encode()


# ---------------------------------------------------------------------------
# get_commit_patch_for_file
# ---------------------------------------------------------------------------


def _commit_with_files(files: list[CommitFile] | None) -> Commit:
    return Commit(id="abc123", message="m", author=None, files=files)


def test_get_commit_patch_for_file_returns_matching_patch():
    class Provider(BaseTestProvider):
        def get_commit(self, sha, request_options=None):
            return _action(
                _commit_with_files(
                    [
                        CommitFile(filename="src/a.py", status="modified", patch="@@ a @@"),
                        CommitFile(filename="src/b.py", status="modified", patch="@@ b @@"),
                    ]
                )
            )

    scm = SourceCodeManager(Provider())
    assert seer.get_commit_patch_for_file(scm, "src/b.py", "abc123") == "@@ b @@"


def test_get_commit_patch_for_file_returns_none_when_not_present():
    class Provider(BaseTestProvider):
        def get_commit(self, sha, request_options=None):
            return _action(_commit_with_files([CommitFile(filename="src/a.py", status="modified", patch="@@ a @@")]))

    scm = SourceCodeManager(Provider())
    assert seer.get_commit_patch_for_file(scm, "src/missing.py", "abc123") is None


def test_get_commit_patch_for_file_handles_none_files():
    class Provider(BaseTestProvider):
        def get_commit(self, sha, request_options=None):
            return _action(_commit_with_files(None))

    scm = SourceCodeManager(Provider())
    assert seer.get_commit_patch_for_file(scm, "src/a.py", "abc123") is None


# ---------------------------------------------------------------------------
# get_valid_file_paths
# ---------------------------------------------------------------------------


def test_get_valid_file_paths_filters_blobs_and_flags_oversized():
    entries = [
        TreeEntry(path="README.md", mode="100644", type="blob", sha="1", size=100),
        TreeEntry(path="big.bin", mode="100644", type="blob", sha="2", size=2_000_000),
        TreeEntry(path="src", mode="040000", type="tree", sha="3", size=None),
        TreeEntry(path="no_size.py", mode="100644", type="blob", sha="4", size=None),
    ]

    class Provider(BaseTestProvider):
        def get_git_commit(self, sha, request_options=None):
            return _action(GitCommitObject(sha=sha, tree=GitCommitTree(sha="tree1"), message="m"))

        def get_tree(self, tree_sha, recursive=True, request_options=None):
            return _action(GitTree(sha=tree_sha, tree=entries, truncated=False))

    scm = SourceCodeManager(Provider())
    valid, oversized = seer.get_valid_file_paths(scm, "abc123", max_file_size=1_000_000)
    assert valid == {"README.md", "big.bin", "no_size.py"}
    assert oversized == {"big.bin"}


# ---------------------------------------------------------------------------
# get_git_tree
# ---------------------------------------------------------------------------


def test_get_git_tree_returns_sha_and_namespaces():
    entries = [
        TreeEntry(path="README.md", mode="100644", type="blob", sha="s1", size=42),
        TreeEntry(path="no_size", mode="100644", type="blob", sha="s2", size=None),
    ]

    class Provider(BaseTestProvider):
        def get_git_commit(self, sha, request_options=None):
            return _action(GitCommitObject(sha=sha, tree=GitCommitTree(sha="treeX"), message="m"))

        def get_tree(self, tree_sha, recursive=True, request_options=None):
            return _action(GitTree(sha=tree_sha, tree=entries, truncated=False))

    scm = SourceCodeManager(Provider())
    tree_sha, ns_iter = seer.get_git_tree(scm, "abc123")
    assert tree_sha == "treeX"
    materialised = list(ns_iter)
    assert [n["sha"] for n in materialised] == ["s1", "s2"]
    assert [n["size"] for n in materialised] == [42, None]
    assert [n["type"] for n in materialised] == ["blob", "blob"]
    assert [n["mode"] for n in materialised] == ["100644", "100644"]
    assert [n["path"] for n in materialised] == ["README.md", "no_size"]


# ---------------------------------------------------------------------------
# _walk_tree_entries (exercised via get_valid_file_paths with truncation)
# ---------------------------------------------------------------------------


def test_walk_tree_entries_truncated_divides_into_subtrees():
    """Root tree is truncated; subtrees return complete listings and paths are joined."""
    root_entries = [
        TreeEntry(path="src", mode="040000", type="tree", sha="src_sha", size=None),
        TreeEntry(path="docs", mode="040000", type="tree", sha="docs_sha", size=None),
        TreeEntry(path="top.py", mode="100644", type="blob", sha="top_sha", size=10),
    ]
    subtrees = {
        "src_sha": [
            TreeEntry(path="a.py", mode="100644", type="blob", sha="a", size=5),
            TreeEntry(path="b.py", mode="100644", type="blob", sha="b", size=5),
        ],
        "docs_sha": [TreeEntry(path="guide.md", mode="100644", type="blob", sha="g", size=5)],
    }

    class Provider(BaseTestProvider):
        def get_git_commit(self, sha, request_options=None):
            return _action(GitCommitObject(sha=sha, tree=GitCommitTree(sha="root_sha"), message="m"))

        def get_tree(self, tree_sha, recursive=True, request_options=None):
            if tree_sha == "root_sha":
                # Recursive listing is truncated; non-recursive listing returns the root entries directly
                return _action(GitTree(sha=tree_sha, tree=root_entries, truncated=recursive))
            return _action(GitTree(sha=tree_sha, tree=subtrees[tree_sha], truncated=False))

    scm = SourceCodeManager(Provider())
    valid, oversized = seer.get_valid_file_paths(scm, "abc123", max_file_size=1_000_000)
    assert valid == {"top.py", "src/a.py", "src/b.py", "docs/guide.md"}
    assert oversized == set()


def test_walk_tree_entries_nested_truncation_recurses():
    """A subtree is itself truncated; the walker recurses into its nested trees."""
    root_entries = [TreeEntry(path="src", mode="040000", type="tree", sha="src_sha", size=None)]
    src_entries = [
        TreeEntry(path="nested", mode="040000", type="tree", sha="nested_sha", size=None),
        TreeEntry(path="top.py", mode="100644", type="blob", sha="t", size=1),
    ]
    nested_entries = [TreeEntry(path="inner.py", mode="100644", type="blob", sha="i", size=1)]

    class Provider(BaseTestProvider):
        def get_git_commit(self, sha, request_options=None):
            return _action(GitCommitObject(sha=sha, tree=GitCommitTree(sha="root_sha"), message="m"))

        def get_tree(self, tree_sha, recursive=True, request_options=None):
            if tree_sha == "root_sha":
                return _action(GitTree(sha=tree_sha, tree=root_entries, truncated=recursive))
            if tree_sha == "src_sha":
                # Truncated under both modes → walker recurses into `nested_sha`
                return _action(GitTree(sha=tree_sha, tree=src_entries, truncated=True))
            return _action(GitTree(sha=tree_sha, tree=nested_entries, truncated=False))

    scm = SourceCodeManager(Provider())
    valid, _ = seer.get_valid_file_paths(scm, "abc123", max_file_size=1_000_000)
    assert "src/top.py" in valid
    assert "src/nested/inner.py" in valid


# ---------------------------------------------------------------------------
# get_commit_history
# ---------------------------------------------------------------------------


def _make_commit(
    sha: str,
    date: datetime | None,
    files: list[CommitFile] | None = None,
    message: str = "msg",
) -> Commit:
    author = None if date is None else CommitAuthor(name="Test User", email="test@example.com", date=date)
    return Commit(id=sha, message=message, author=author, files=files)


class _CommitHistoryProvider(BaseTestProvider):
    """
    Returns the same flat list of commits on every call, regardless of the `cursor`
    value. `get_commit_history` slices client-side via `start_index:end_index`, so a
    single-page provider is enough to exercise its pagination logic. Honors
    `since` / `until` server-side, mirroring the real providers.
    """

    def __init__(self, commits: list[Commit]):
        self._commits = commits
        self._commits_by_sha = {c["id"]: c for c in commits}

    def _filter(self, since: datetime | None, until: datetime | None) -> list[Commit]:
        result: list[Commit] = []
        for commit in self._commits:
            author = commit["author"]
            commit_date = author["date"] if author else None
            if since is not None and (commit_date is None or commit_date < since):
                continue
            if until is not None and (commit_date is None or commit_date > until):
                continue
            result.append(commit)
        return result

    def get_commits_by_path(
        self,
        path: str,
        ref: str | None = None,
        pagination: PaginationParams | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        request_options: RequestOptions | None = None,
    ):
        return _paginated(self._filter(since, until), next_cursor=None)

    def get_commit(self, sha, request_options=None):
        commit = self._commits_by_sha[sha]
        return _action(
            _make_commit(
                sha=commit["id"],
                date=commit["author"]["date"] if commit["author"] else None,
                files=commit.get("files") or [CommitFile(filename="fallback.py", status="modified", patch=None)],
                message=commit["message"],
            )
        )


def test_get_commit_history_formats_one_block_per_commit():
    files = [CommitFile(filename="src/main.py", status="modified", patch="@@ @@")]
    commits = [
        _make_commit("aaaaaaaaaaaa", datetime(2026, 1, 1, tzinfo=UTC), files=files, message="first"),
        _make_commit("bbbbbbbbbbbb", datetime(2026, 1, 2, tzinfo=UTC), files=files, message="second"),
    ]
    scm = SourceCodeManager(_CommitHistoryProvider(commits))

    blocks = seer.get_commit_history(scm, path="src/main.py", sha="HEAD", build_file_tree_string=_build_file_tree)
    assert len(blocks) == 2
    assert "aaaaaaa - first (2026-01-01)" in blocks[0]
    assert "bbbbbbb - second (2026-01-02)" in blocks[1]
    assert "Test User <test@example.com>" in blocks[0]
    assert "modified src/main.py" in blocks[0]


def test_get_commit_history_respects_pagination():
    files = [CommitFile(filename="f.py", status="modified", patch=None)]
    commits = [
        _make_commit(str(i) * 12, datetime(2026, 1, i + 1, tzinfo=UTC), files=files, message=f"m{i}") for i in range(5)
    ]

    class CursorAwareProvider(_CommitHistoryProvider):
        """Honors cursor as a 1-indexed page number, paging through `commits`."""

        def get_commits_by_path(
            self,
            path: str,
            ref: str | None = None,
            pagination: PaginationParams | None = None,
            since: datetime | None = None,
            until: datetime | None = None,
            request_options: RequestOptions | None = None,
        ):
            filtered = self._filter(since, until)
            per_page = pagination["per_page"] if pagination else 50
            cursor = int(pagination["cursor"]) if pagination else 1
            start = (cursor - 1) * per_page
            end = start + per_page
            next_cursor = str(cursor + 1) if end < len(filtered) else None
            return _paginated(filtered[start:end], next_cursor=next_cursor)

    scm = SourceCodeManager(CursorAwareProvider(commits))

    page1 = seer.get_commit_history(
        scm, path="f.py", sha="HEAD", build_file_tree_string=_build_file_tree, max_commits=2, page=1
    )
    page2 = seer.get_commit_history(
        scm, path="f.py", sha="HEAD", build_file_tree_string=_build_file_tree, max_commits=2, page=2
    )
    assert len(page1) == 2
    assert len(page2) == 2
    assert "m0" in page1[0] and "m1" in page1[1]
    assert "m2" in page2[0] and "m3" in page2[1]


def test_get_commit_history_filters_by_since_and_until():
    files = [CommitFile(filename="f.py", status="modified", patch=None)]
    commits = [
        _make_commit("aaaaaaaaaaaa", datetime(2026, 1, 1, tzinfo=UTC), files=files, message="old"),
        _make_commit("bbbbbbbbbbbb", datetime(2026, 2, 1, tzinfo=UTC), files=files, message="mid"),
        _make_commit("cccccccccccc", datetime(2026, 3, 1, tzinfo=UTC), files=files, message="new"),
    ]
    scm = SourceCodeManager(_CommitHistoryProvider(commits))

    blocks = seer.get_commit_history(
        scm,
        path="f.py",
        sha="HEAD",
        build_file_tree_string=_build_file_tree,
        since=datetime(2026, 1, 15, tzinfo=UTC),
        until=datetime(2026, 2, 15, tzinfo=UTC),
    )
    assert len(blocks) == 1
    assert "mid" in blocks[0]


def test_get_commit_history_skips_missing_author_dates_when_filtering():
    files = [CommitFile(filename="f.py", status="modified", patch=None)]
    commits = [_make_commit("aaaaaaaaaaaa", date=None, files=files, message="no-author")]
    scm = SourceCodeManager(_CommitHistoryProvider(commits))

    blocks = seer.get_commit_history(
        scm,
        path="f.py",
        sha="HEAD",
        build_file_tree_string=_build_file_tree,
        since=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert blocks == []


def test_get_commit_history_falls_back_to_get_commit_for_missing_files():
    """Commits returned without files should trigger a secondary fetch."""
    authored = datetime(2026, 1, 1, tzinfo=UTC)
    listing = [_make_commit("aaaaaaaaaaaa", authored, files=None, message="needs-fetch")]
    scm = SourceCodeManager(_CommitHistoryProvider(listing))

    blocks = seer.get_commit_history(scm, path="f.py", sha="HEAD", build_file_tree_string=_build_file_tree)
    assert len(blocks) == 1
    # The fallback commit returned by _CommitHistoryProvider.get_commit provides "fallback.py"
    assert "fallback.py" in blocks[0]


def test_get_commit_history_caps_files_per_commit():
    """More than MAX_COMMIT_FILES (20) files should be summarised with an 'and N more files' note."""
    files = [CommitFile(filename=f"f{i}.py", status="modified", patch=None) for i in range(25)]
    authored = datetime(2026, 1, 1, tzinfo=UTC)
    commits = [_make_commit("aaaaaaaaaaaa", authored, files=files, message="big")]
    scm = SourceCodeManager(_CommitHistoryProvider(commits))

    [block] = seer.get_commit_history(scm, path="f.py", sha="HEAD", build_file_tree_string=_build_file_tree)
    assert "and 5 more files were changed" in block


def test_get_commit_history_handles_missing_author_entirely():
    files = [CommitFile(filename="f.py", status="modified", patch=None)]
    commits = [_make_commit("aaaaaaaaaaaa", date=None, files=files, message="anon")]
    scm = SourceCodeManager(_CommitHistoryProvider(commits))

    [block] = seer.get_commit_history(scm, path="f.py", sha="HEAD", build_file_tree_string=_build_file_tree)
    assert "unknown" in block  # both date and author name render as "unknown"
