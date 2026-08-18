# Introduction

"Actions" below refer to free functions in `src/scm/actions.py`.
They correspond to protocol abilities defined in `src/scm/types.py` and implemented in `src/scm/providers`.

This file lists the API they call.
For more details, refer to the implementation.

The note "*not implemented*" makes no assumption about our ability to do it.
It just shows that we've not done it yet. On the contrary, *not supported* means that this provider does not have a close enough functionality.

<!--
    Note to maintainers:
    - Actions are kept in alphabetical order.
    - There may be errors in links to API docs. Please fix them if you find any.
    - Keep this file terse.
    - This file currently documents Cursor Origin only. The Bitbucket branch
      (jacquev6/document-for-bitbucket) introduces the same file with GitHub, GitLab and
      Bitbucket columns; merging the two is a mechanical interleave, one `##` section at
      a time.
-->

Cursor Origin's base URL is `https://api.cursor.com/v1/origin` and every repository path below is
relative to it. `{o}/{r}` is `ownerSlug/repoName` — the repository's `fullName`. The authoritative
description of the API is its OpenAPI document, [`openapi.yaml`](https://cursor.com/docs/api/origin/openapi.yaml);
the [prose docs](https://cursor.com/docs/api/origin) are a summary of it and are wrong about at
least one path (see `get_file_content`).

Every endpoint below was exercised against `sentry/nuget-trends` unless marked otherwise.

*not supported* here means **the published API does not offer it**, which is not always the same as
Origin not having it — the web UI has reactions, for instance. We build to the spec rather than to
probed-out endpoints; see the "Scope" and "Revisit when Origin supports it" sections of
[limitations.md](limitations.md).

# Actions

## `collapse_pull_request_comment`

Cursor Origin: *not supported* (no review threads, no resolve or minimize)

## `compare_commits`

Cursor Origin:
- [GET /repos/{o}/{r}/compare/{start_sha}...{end_sha}](https://cursor.com/docs/api/origin) (counts and boundary commits only)
- [GET /repos/{o}/{r}/commits?sha={end_sha}](https://cursor.com/docs/api/origin) (walked back to the merge base to build the commit list)

## `create_branch`

Cursor Origin: *not supported* (branches are read-only over REST; push over Git HTTPS)

## `create_check_run`

Cursor Origin: [POST /repos/{o}/{r}/check-runs](https://cursor.com/docs/api/origin) (creates the check suite and the run in one call)

## `create_commit`

Cursor Origin: *not supported* (no contents-write, blob, tree, or commit endpoint; commit over Git HTTPS)

## `create_git_blob`

Cursor Origin: *not supported* (the git database is read-only)

## `create_git_commit`

Cursor Origin: *not supported* (the git database is read-only)

## `create_git_tree`

Cursor Origin: *not supported* (the git database is read-only)

## `create_issue`

Cursor Origin: *not supported* (no issue tracker)

## `create_issue_comment`

Cursor Origin: *not supported* (no issue tracker)

## `create_issue_comment_reaction`

Cursor Origin: *not supported* (no issue tracker; and the API exposes no reactions — see limitations.md)

## `create_issue_reaction`

Cursor Origin: *not supported* (no issue tracker; and the API exposes no reactions — see limitations.md)

## `create_pull_request`

Cursor Origin: [POST /repos/{o}/{r}/pulls](https://cursor.com/docs/api/origin)

## `create_pull_request_comment`

Cursor Origin: [POST /repos/{o}/{r}/pulls/{n}/comments](https://cursor.com/docs/api/origin)

## `create_pull_request_comment_reaction`

Cursor Origin: *not supported* (the API exposes no reactions; the web UI does have them — see limitations.md)

## `create_pull_request_draft`

Cursor Origin: [POST /repos/{o}/{r}/pulls](https://cursor.com/docs/api/origin) with `draft: true`

## `create_pull_request_reaction`

Cursor Origin: *not supported* (the API exposes no reactions; the web UI does have them — see limitations.md)

## `create_review`

Cursor Origin:
- [POST /repos/{o}/{r}/pulls/{n}/reviews](https://cursor.com/docs/api/origin) (verdict and body only)
- [POST /repos/{o}/{r}/pulls/{n}/comments](https://cursor.com/docs/api/origin) once per inline comment, degraded to a located general comment (see limitations.md). Not atomic; the verdict is posted first.

## `create_review_comment`

Cursor Origin: [POST /repos/{o}/{r}/pulls/{n}/comments](https://cursor.com/docs/api/origin) — **degraded**. Origin has no diff anchor, so the comment states its file and line in a header line instead. See limitations.md.

## `create_review_comment_file`

Cursor Origin: [POST /repos/{o}/{r}/pulls/{n}/comments](https://cursor.com/docs/api/origin) — **degraded**, as `create_review_comment` but with no line.

## `create_review_comment_reaction`

Cursor Origin: *not supported* (the API exposes no reactions; the web UI does have them — see limitations.md)

## `create_review_comment_reply`

Cursor Origin:
- [GET /repos/{o}/{r}/pulls/comments/{comment_id}](https://cursor.com/docs/api/origin) (to recover the thread id)
- [POST /repos/{o}/{r}/pulls/{n}/comments](https://cursor.com/docs/api/origin) with `threadId`

Threading is native here: Origin threads by thread id rather than by parent comment.

## `delete_branch`

Cursor Origin: *not supported* (branches are read-only over REST)

## `delete_issue_comment`

Cursor Origin: *not supported* (no issue tracker)

## `delete_issue_comment_reaction`

Cursor Origin: *not supported* (no issue tracker; and the API exposes no reactions — see limitations.md)

## `delete_issue_reaction`

Cursor Origin: *not supported* (no issue tracker; and the API exposes no reactions — see limitations.md)

## `delete_pull_request_comment`

Cursor Origin: *not supported* (comments can be edited, not deleted)

## `delete_pull_request_comment_reaction`

Cursor Origin: *not supported* (the API exposes no reactions; the web UI does have them — see limitations.md)

## `delete_pull_request_reaction`

Cursor Origin: *not supported* (the API exposes no reactions; the web UI does have them — see limitations.md)

## `delete_review_comment_reaction`

Cursor Origin: *not supported* (the API exposes no reactions; the web UI does have them — see limitations.md)

## `download_archive`

Cursor Origin: *not supported* (no archive endpoint; clone over Git HTTPS)

## `download_workflow_job_log`

Cursor Origin: *not supported* (no CI surface of its own; only check runs reported by others)

## `get_app_installation`

Cursor Origin: *not implemented* — [GET /app/installations/{id}](https://cursor.com/docs/api/origin)
returns the granted `scopes`, but it needs the app JWT *and* the installation id, and the provider is
constructed with neither. See limitations.md.

## `get_archive_link`

Cursor Origin: *not supported* (no archive endpoint)

## `get_authenticated_actor`

Cursor Origin: [GET /app](https://cursor.com/docs/api/origin) (app JWT; returns the app's own id and slug)

## `get_branch`

Cursor Origin: [GET /repos/{o}/{r}/git/ref/heads/{branch}](https://cursor.com/docs/api/origin)
(there is no `/branches/{name}` route; `/branches` lists and cannot filter)

## `get_check_run`

Cursor Origin: [GET /repos/{o}/{r}/check-runs/{check_run_id}](https://cursor.com/docs/api/origin)

## `get_commit`

Cursor Origin:
- [GET /repos/{o}/{r}/commits/{sha}](https://cursor.com/docs/api/origin)
- [GET /repos/{o}/{r}/commits/{sha}/files](https://cursor.com/docs/api/origin) (walked to exhaustion; the commit response carries no files)

## `get_commit_changes`

Cursor Origin: [GET /repos/{o}/{r}/commits/{sha}/files](https://cursor.com/docs/api/origin)

## `get_commit_url`

Cursor Origin: built locally from the web base URL — **unverified**, see limitations.md

## `get_commits`

Cursor Origin: [GET /repos/{o}/{r}/commits?sha={ref}](https://cursor.com/docs/api/origin)
(`since`/`until` are rejected: Origin ignores them rather than filtering)

## `get_commits_by_path`

Cursor Origin: *not supported* (`path` is accepted and ignored, so there is no way to scope a commit list to a file)

## `get_commits_url`

Cursor Origin: built locally from the web base URL — **unverified**, see limitations.md

## `get_directory_contents`

Cursor Origin: [GET /repos/{o}/{r}/contents?path={path}&ref={ref}](https://cursor.com/docs/api/origin)
(a directory response carries an `entries` array)

## `get_file_content`

Cursor Origin: [GET /repos/{o}/{r}/contents?path={path}&ref={ref}](https://cursor.com/docs/api/origin)

⚠️ A **query parameter**, not `/contents/{path}`. The path form 404s with `"Route GET:... not found"`,
which reads like a permissions failure and is not one.

## `get_file_url`

Cursor Origin: built locally from the web base URL — **unverified**, see limitations.md

## `get_full_tree`

Cursor Origin: [GET /repos/{o}/{r}/git/trees/{sha}?recursive=1](https://cursor.com/docs/api/origin)

## `get_git_commit`

Cursor Origin: [GET /repos/{o}/{r}/git/commits/{sha}](https://cursor.com/docs/api/origin)

## `get_git_ref`

Cursor Origin: [GET /repos/{o}/{r}/git/ref/{ref}](https://cursor.com/docs/api/origin) (`heads/x`, `tags/x`, or `HEAD`)

## `get_issue`

Cursor Origin: *not supported* (no issue tracker)

## `get_issue_comment_reactions`

Cursor Origin: *not supported* (no issue tracker; and the API exposes no reactions — see limitations.md)

## `get_issue_comments`

Cursor Origin: *not supported* (no issue tracker)

## `get_issue_reactions`

Cursor Origin: *not supported* (no issue tracker; and the API exposes no reactions — see limitations.md)

## `get_pull_request`

Cursor Origin: [GET /repos/{o}/{r}/pulls/{n}](https://cursor.com/docs/api/origin)

## `get_pull_request_comment_reactions`

Cursor Origin: *not supported* (the API exposes no reactions; the web UI does have them — see limitations.md)

## `get_pull_request_comments`

Cursor Origin: [GET /repos/{o}/{r}/pulls/{n}/comments](https://cursor.com/docs/api/origin)

## `get_pull_request_commits`

Cursor Origin: [GET /repos/{o}/{r}/pulls/{n}/commits](https://cursor.com/docs/api/origin)

## `get_pull_request_diff`

Cursor Origin: *not implemented* — there is no diff route. `pulls/{n}/files` carries a per-file
`patch`, so a unified diff could be reassembled from it; nothing needs that yet.

## `get_pull_request_files`

Cursor Origin: [GET /repos/{o}/{r}/pulls/{n}/files](https://cursor.com/docs/api/origin)

## `get_pull_request_reactions`

Cursor Origin: *not supported* (the API exposes no reactions; the web UI does have them — see limitations.md)

## `get_pull_request_review`

Cursor Origin: *not implemented* — reviews can only be listed, not fetched by id, so this would be a
list-and-scan.

## `get_pull_request_review_threads`

Cursor Origin: *not implemented* — comments expose a `thread.id`, but the threads are
general-discussion, carry no file or line, and cannot be resolved. See limitations.md.

## `get_pull_request_template`

Cursor Origin: *not implemented* (`get_directory_contents` + `get_file_content` would work; no
convention is established for Origin yet)

## `get_pull_request_url`

Cursor Origin: built locally from the web base URL — `/pull/{n}`, confirmed in the web UI (the plural
`/pulls/{n}` is a dead link). See limitations.md.

## `get_pull_requests`

Cursor Origin: [GET /repos/{o}/{r}/pulls?state={state}&head={branch}](https://cursor.com/docs/api/origin)

## `get_readme`

Cursor Origin: [GET /repos/{o}/{r}/contents](https://cursor.com/docs/api/origin) (root listing, matched
by name) then [GET /repos/{o}/{r}/contents?path={readme}](https://cursor.com/docs/api/origin).
There is no `/readme` endpoint.

## `get_repository`

Cursor Origin: [GET /repos/{o}/{r}](https://cursor.com/docs/api/origin)

## `get_repository_assignees`

Cursor Origin: *not supported* (no assignees; reviewer operations are webhook-only)

## `get_repository_labels`

Cursor Origin: *not supported* (no labels)

## `get_repository_topics`

Cursor Origin: *not supported* (no topics)

## `get_repository_user_permission`

Cursor Origin: *not supported* (no collaborator or permission endpoint)

## `get_review_comment_reactions`

Cursor Origin: *not supported* (the API exposes no reactions; the web UI does have them — see limitations.md)

## `get_review_comments`

Cursor Origin: *not supported* (reviews have no inline comments)

## `get_thread_id_from_review_comment_unique_id`

Cursor Origin: *not supported* (no review threads)

## `get_tree`

Cursor Origin: [GET /repos/{o}/{r}/git/trees/{sha}?recursive=1](https://cursor.com/docs/api/origin)

⚠️ Any non-empty `recursive` value — including `"false"` — turns recursion on, so the parameter is
omitted rather than set to a falsy value.

## `list_check_runs_for_ref`

Cursor Origin: [GET /repos/{o}/{r}/commits/{ref}/check-runs](https://cursor.com/docs/api/origin)
(the `check_name`/`status` filters are applied client-side)

## `list_check_runs_in_check_suite`

Cursor Origin: [GET /repos/{o}/{r}/check-suites/{id}/check-runs](https://cursor.com/docs/api/origin)
(the `check_name`/`status` filters are applied client-side)

## `list_pull_request_reviews`

Cursor Origin: [GET /repos/{o}/{r}/pulls/{n}/reviews](https://cursor.com/docs/api/origin)

## `list_repositories`

Cursor Origin: [GET /installation/repos](https://cursor.com/docs/api/origin)

## `list_repository_user_permissions`

Cursor Origin: *not supported* (no collaborator endpoint)

## `list_workflow_jobs`

Cursor Origin: *not supported* (no CI surface)

## `list_workflow_runs`

Cursor Origin: *not supported* (no CI surface)

## `mark_pull_request_as_draft`

Cursor Origin: [PATCH /repos/{o}/{r}/pulls/{n}](https://cursor.com/docs/api/origin) with `draft: true`

## `mark_pull_request_ready_for_review`

Cursor Origin: [PATCH /repos/{o}/{r}/pulls/{n}](https://cursor.com/docs/api/origin) with `draft: false`

## `minimize_comment`

Cursor Origin: *not supported* (no moderation surface)

## `request_review`

Cursor Origin: *not supported* (reviewer operations are webhook-only; there is no API to add one)

## `resolve_review_thread`

Cursor Origin: *not supported* (no thread resolution)

## `update_and_collapse_pull_request_comment`

Cursor Origin: *not supported* (nothing to collapse)

## `update_branch`

Cursor Origin: *not supported* (branches are read-only over REST)

## `update_check_run`

Cursor Origin:
- [GET /repos/{o}/{r}/check-runs/{id}](https://cursor.com/docs/api/origin) and
  [GET /repos/{o}/{r}/check-suites/{id}](https://cursor.com/docs/api/origin) (to recover the keys and head sha)
- [POST /repos/{o}/{r}/check-runs](https://cursor.com/docs/api/origin) (upsert; there is no PATCH)

## `update_issue`

Cursor Origin: *not supported* (no issue tracker)

## `update_pull_request`

Cursor Origin: [PATCH /repos/{o}/{r}/pulls/{n}](https://cursor.com/docs/api/origin)

## `update_review_comment`

Cursor Origin: [PATCH /repos/{o}/{r}/pulls/comments/{comment_id}](https://cursor.com/docs/api/origin) (author-only). With no resolve or collapse, editing in place is the only way to retract a superseded finding.
