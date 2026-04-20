import base64
import logging
import os
import textwrap
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from scm.manager import SourceCodeManager
from scm.rpc.client import SourceCodeManager as ScmRpcClient
from scm.types import (
    SHA,
    Commit,
    GetCommitProtocol,
    GetCommitsByPathProtocol,
    GetFileContentProtocol,
    GetGitCommitProtocol,
    GetTreeProtocol,
    TreeEntry,
)

logger = logging.getLogger(__name__)


SCM = SourceCodeManager | ScmRpcClient


class SeerCapabilities(
    GetCommitProtocol,
    GetCommitsByPathProtocol,
    GetFileContentProtocol,
    GetGitCommitProtocol,
    GetTreeProtocol,
): ...


def get_file_content(scm: SeerCapabilities, path: str, sha: str) -> bytes:
    file_content = scm.get_file_content(path=path, ref=sha)["data"]
    if file_content["encoding"] == "base64":
        return base64.b64decode(file_content["content"])
    else:
        return file_content["content"].encode("utf-8")


def get_commit_patch_for_file(scm: SeerCapabilities, path: str, commit_sha: str) -> str | None:
    for file in scm.get_commit(commit_sha)["data"]["files"] or []:
        if file["filename"] == path:
            return file["patch"]
    return None


def get_valid_file_paths(scm: SeerCapabilities, commit_sha: SHA, max_file_size: int) -> tuple[set[str], set[str]]:
    git_commit = scm.get_git_commit(commit_sha)["data"]

    valid_file_paths: set[str] = set()
    oversized_file_paths: set[str] = set()
    for entry in _walk_tree_entries(scm, git_commit["tree"]["sha"]):
        if entry["type"] != "blob":
            continue
        valid_file_paths.add(entry["path"])
        size = entry.get("size")
        if size is not None and size > max_file_size:
            oversized_file_paths.add(entry["path"])

    return (valid_file_paths, oversized_file_paths)


def get_git_tree(scm: SeerCapabilities, commit_sha: str) -> tuple[SHA, list[TreeEntry]]:
    """
    Fetch the full git tree for a commit via the SCM client. Truncation is
    handled by _walk_tree_entries (divide and conquer across subtrees).
    """
    git_commit = scm.get_git_commit(commit_sha)["data"]
    return (git_commit["tree"]["sha"], _walk_tree_entries(scm, git_commit["tree"]["sha"]))


def _walk_tree_entries(scm: SeerCapabilities, tree_sha: str) -> list[TreeEntry]:
    """
    Fetch every entry under a tree, with paths relative to that tree. Falls back to
    a divide-and-conquer subtree walk when the recursive listing is truncated.
    """
    tree = scm.get_tree(tree_sha, recursive=True)["data"]
    if not tree["truncated"]:
        return list(tree["tree"])

    def walk(sha: str, parent_path: str) -> list[TreeEntry]:
        subtree = scm.get_tree(sha, recursive=True)["data"]
        if not subtree["truncated"]:
            return [{**entry, "path": os.path.join(parent_path, entry["path"])} for entry in subtree["tree"]]
        inner = scm.get_tree(sha, recursive=False)["data"]
        out: list[TreeEntry] = []
        for entry in inner["tree"]:
            full_path = os.path.join(parent_path, entry["path"])
            if entry["type"] == "tree":
                out.extend(walk(entry["sha"], full_path))
            out.append({**entry, "path": full_path})
        return out

    root = scm.get_tree(tree_sha, recursive=False)["data"]
    entries: list[TreeEntry] = list(root["tree"])
    subtree_jobs = [(entry["sha"], entry["path"]) for entry in root["tree"] if entry["type"] == "tree"]

    with ThreadPoolExecutor() as executor:
        for batch in executor.map(lambda job: walk(job[0], job[1]), subtree_jobs):
            entries.extend(batch)

    return entries


def get_commit_history(
    scm: SeerCapabilities,
    path: str,
    sha: SHA,
    build_file_tree_string: Callable[[list[dict[str, str]]], str],
    max_commits: int = 10,
    page: int = 1,
    since: datetime | None = None,
    until: datetime | None = None,
) -> list[str]:
    # Calculate which commits we want (0-based indexing)
    start_commit_index = (page - 1) * max_commits
    end_commit_index = start_commit_index + max_commits

    # Calculate which pages we need to fetch
    default_per_page = 30
    start_page = start_commit_index // default_per_page
    end_page = (end_commit_index - 1) // default_per_page

    # Collect commits from the required pages
    all_commits: list[Commit] = []
    for github_page in range(start_page, end_page + 1):
        page_commits = scm.get_commits_by_path(
            path=path,
            ref=sha,
            pagination={"cursor": str(github_page + 1), "per_page": default_per_page},
            since=since,
            until=until,
        )
        all_commits.extend(page_commits["data"])
        # Stop early if we've collected enough commits
        if len(all_commits) >= end_commit_index - start_page * default_per_page:
            break

    # Extract the specific range we want
    start_offset = start_commit_index - start_page * default_per_page
    end_offset = start_offset + max_commits
    commit_list = all_commits[start_offset:end_offset]

    def process_commit(commit: Commit) -> str:
        MAX_COMMIT_FILES = 20

        commit_sha = commit["id"]
        files = commit.get("files")
        if not files:
            commit = scm.get_commit(commit_sha)["data"]
            files = commit.get("files") or []

        short_sha = commit_sha[:7]
        message = commit["message"]
        author = commit.get("author")
        if author and author["date"] is not None:
            commit_date = author["date"].strftime("%Y-%m-%d")
        else:
            commit_date = "unknown"
        author_name = author["name"] if author else "unknown"
        author_email = author["email"] if author else ""

        raw_files = files[:MAX_COMMIT_FILES]
        files_touched = [{"path": f["filename"], "status": f["status"]} for f in raw_files]

        file_tree_str = build_file_tree_string(files_touched)

        total_files_count = len(files)
        additional_files_note = ""
        if len(files_touched) < total_files_count:
            additional_files_note = f"\n[and {total_files_count - len(files_touched)} more files were changed...]"

        return textwrap.dedent(
            """\
            ----------------
            {short_sha} - {message} ({date})
            Author: {author_name} <{author_email}>
            Files touched:
            {file_tree}{additional_files}
            """
        ).format(
            short_sha=short_sha,
            message=message,
            date=commit_date,
            author_name=author_name,
            author_email=author_email,
            file_tree=file_tree_str,
            additional_files=additional_files_note,
        )

    with ThreadPoolExecutor() as executor:
        results = list(executor.map(process_commit, commit_list))

    return list(results)
