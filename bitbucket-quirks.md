# Bitbucket quirks

Corner cases and limitations discovered while implementing the Bitbucket provider.
These are behaviors that diverge from GitHub/GitLab or from what the generic action signatures suggest.

## `get_authenticated_actor`

- **Returns the human account, not a bot.** Unlike GitHub (which resolves the app's bot user), `GET /user` returns the Atlassian account tied to the credentials, so it requires user-level auth (email + API token, or OAuth with the `account` scope) and 403s for repository/project access tokens.
- **`username` is the nickname.** Bitbucket dropped real usernames for privacy, so `Author.username` is `nickname` (falling back to `display_name`) and `Author.id` is the opaque, brace-wrapped account `uuid`.

## `get_thread_id_from_review_comment_unique_id`

- **Local, no API call.** Like GitLab, this needs no request: a Bitbucket comment roots its own thread (`map_review_comment` sets `thread_id` == the comment id), so the review comment's `unique_id` already *is* the thread id and is returned unchanged.

## `resolve_review_thread` / `collapse_pull_request_comment` / `update_and_collapse_pull_request_comment`

- **Resolves the thread, ignores `reason` and `comment_node_id`.** Bitbucket resolves a comment *thread* via `POST .../comments/{comment_id}/resolve`, where the comment is the thread root (so `thread_id` is the root comment's id). `resolve_review_thread` is the primitive; `collapse_pull_request_comment` delegates to it. Bitbucket has no minimize-with-reason concept, so `reason` and the GitHub-only `comment_node_id` are ignored.
- **Resolving is not idempotent.** Re-resolving an already-resolved thread returns `409 Conflict` (`"Comment has already been resolved."`), unlike GitLab (`PUT resolved=true`) and GitHub (`resolveReviewThread`), which are safe to repeat.
- **`update_and_collapse` is not atomic.** Bitbucket has no combined edit-and-resolve endpoint (unlike GitHub's single GraphQL mutation). We `PUT` the comment body and then `POST` the resolve as two separate requests, so the edit can succeed while the resolve fails.

## `get_repository_assignees`

- **No assignee concept; returns users with repo permission.** Bitbucket has no assignable-users list, so we return every user with an explicit permission on the repository (`GET /workspaces/{workspace}/permissions/repositories/{repo_slug}`), mapping each entry's `user` to an `Author`. Permissions are effective (highest of direct/group), and the list is not filtered by permission level.
- **Requires admin.** Only a caller with admin permission on the repository may read this endpoint; others get `403`.

## `get_app_installation`

- **No app-installation concept.** Bitbucket has nothing equivalent to a GitHub App installation, and the per-repository permission endpoint (`GET /user/permissions/repositories`) was deprecated (CHANGE-2770, returns `410 Gone`). The closest remaining signal is the authenticated user's *workspace-level* role, from `GET /user/workspaces/{workspace}/permission`.
- **Coarse, workspace-wide access.** The role is one of `owner` > `member` > `collaborator`. We map `owner`/`member` to write access and `collaborator` to read-only. This is coarser than a per-repo permission: it reflects the workspace role, not any per-repo override.
- **No check-run permission.** Bitbucket has no separate permission for commit build statuses; they only need repository write access, so `has_check_run_write_access` mirrors `has_write_access`.

## `update_pull_request`

- **Cannot reopen a pull request.** Bitbucket has no API to reopen a declined  pull request, so `state="open"` raises `ResourceBadRequest`.
Closing works via the separate `POST .../pullrequests/{id}/decline` endpoint (there is no `state` field on the PUT body).

## `get_commits`

- **No date filtering.** Bitbucket's commits endpoint supports neither `since`/`until` params nor `q` date filtering, so passing `since` or `until` raises `ResourceBadRequest` rather than silently returning unfiltered commits.

## `compare_commits`

- **No single compare endpoint.** We combine the commits endpoint (`{end_sha}` excluding `{start_sha}`) with the diffstat endpoint. The commit list is paginated via the cursor; the diffstat is walked in full so the changed-file list is complete, and `ahead_by` reflects only the current commit page.
- **Diffstat `spec` order is opposite git.** Bitbucket reads `A..B` as "preview A's changes against baseline B", so we pass `{end_sha}..{start_sha}` to get git's `start..end` diff. `diff` entries come from diffstat, so `patch` is always `None` (counts only).

## `get_pull_request_files`

- **No patch text.** Backed by Bitbucket's diffstat endpoint, which reports per-file line counts but no diff hunks, so `patch` is always `None` (GitHub/GitLab populate it) and `sha` is empty.

## `create_check_run` / `get_check_run` / `update_check_run`

- **Mapped to commit build statuses.** Check runs are Bitbucket commit build statuses, keyed per commit. The `check_run_id` is `"{sha}:{key}"`, where `key` is the caller's `external_id` (falling back to `name`).
- **Reduced fidelity.** States collapse to INPROGRESS/SUCCESSFUL/FAILED/STOPPED; `started_at`/`completed_at` are ignored; only the output title is forwarded (as the status `description`). Bitbucket requires a link, so `url` defaults to the commit page.
- **40-char name/key limit.** Bitbucket caps the build status key at 40 characters. Since we derive the key from `external_id` (falling back to `name`), a name/external_id longer than 40 chars is rejected.

## `create_review`

- **Not atomic.** Bitbucket has no single review endpoint (unlike GitHub). The inline comments, review body, and approval are separate requests fanned out concurrently, so individual calls can succeed independently. Only `event == "approve"` triggers an action (the approve endpoint); `"comment"`/`"change_request"` just post comments. The returned `Review` has a placeholder `id` of `"unset"`.

## `get_file_content`

- **Blob SHA computed locally.** Bitbucket's `/src` endpoint returns raw bytes with no git blob id, so we recompute it as `sha1("blob <len>\0" + content)` to match the blob SHA GitHub/GitLab report. Content is base64-encoded and `size` is derived from the response's byte length.

## `get_commits_by_path`

- **No date filtering.** Same as `get_commits`: the file-history endpoint has no date filter, so `since`/`until` are rejected.
- **`ref` is effectively required.** The `/filehistory/{commit}/{path}` endpoint needs a commit in the URL, so when `ref` is omitted we spend an extra call resolving the repository's default branch.
- **N+1 to hydrate commits.** File-history entries embed only an abbreviated commit (hash + `links`, no message/author/date). We follow each entry's commit hash with a `GET .../commit/{hash}` call to get the full commit, so a page of N entries costs N+1 requests. The N hydration calls run concurrently via a thread pool.
