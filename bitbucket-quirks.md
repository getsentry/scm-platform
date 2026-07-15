# Bitbucket quirks

Corner cases and limitations discovered while implementing the Bitbucket provider.
These are behaviors that diverge from GitHub/GitLab or from what the generic action signatures suggest.

## `get_pull_request_template`

- **A single template file, not a directory.** Unlike GitHub (multiple parent dirs) and GitLab (a `merge_request_templates/` directory), Bitbucket Cloud reads exactly one file, `.bitbucket/pull_request_template.md`, so the iterator yields at most one `FileContent`. The template is read from the source branch, so `ref` should be the PR's source branch; `pagination` is unused. A missing file yields nothing.

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
- **Refs containing `/` need resolving.** `/src/{commit}/{path}` parses the commit only up to the first `/`, so a branch/tag name like `topics/templates` is truncated to `topics` ("Commit not found") -- even when the slash is URL-encoded as `%2F`. We resolve a slash-containing ref to its commit hash (via `/refs/branches/{ref}`, falling back to `/refs/tags/{ref}`) before calling `/src`, at the cost of one extra request. Slash-free refs pass through unchanged.

## `get_tree`

- **No tree-object endpoint; lists via `/src`.** Bitbucket has nothing like GitHub's `git/trees/{sha}`. We list the repository root through the `/src/{commit}/` browsing endpoint (trailing slash required for the root), which -- like GitLab -- takes a ref/commit rather than a tree-object SHA. So `tree_sha` is treated as a ref, and a slash-containing one is resolved to a commit hash via `_ref_to_commit` first (the `{commit}` segment is parsed only up to the first `/`, same as `get_file_content`).
- **No blob/tree SHA.** `/src` listings carry no git object id, so every `TreeEntry.sha` is empty (like `get_pull_request_files`). `GitTree.sha` echoes the input `tree_sha` (as GitLab does), not a real tree-object SHA.
- **Recursion is best-effort via `max_depth`.** `recursive=True` passes `max_depth=100` for Bitbucket's breadth-first walk; directories nested deeper than that are returned as `commit_directory` entries but not descended into. Bitbucket returns `555` if the value is "too large" for the repo, so very large trees may fail rather than truncate. `recursive=False` lists only the direct root children (Bitbucket's `max_depth=1` default).
- **In-body pagination with an opaque cursor.** Like other Bitbucket lists, `/src` paginates with `values` + a `next` URL, so a page carries `meta["next_cursor"]` and `truncated` mirrors whether more pages remain (unlike GitHub, which returns the whole tree in one response and self-reports `truncated`). Unlike Bitbucket's numeric-`page` endpoints, though, `/src` pages by an *opaque* token, so a synthetic `page=1` is rejected with `400 "Invalid page"` -- the first page must omit `page` entirely (our `get()` only forwards `page` when a `cursor` is actually present). The default page size is 10.
- **Mode from `attributes`.** File modes are derived from each entry's `attributes` list: `subrepository` -> `160000` (`commit`), `link` -> `120000`, `executable` -> `100755`, otherwise `100644`; directories are `040000`.

## `get_full_tree`

- **Walks `/src` pages to exhaustion.** Same endpoint and mapping as `get_tree`, but it follows the in-body `next` cursor until there are no more pages, concatenating the entries into one non-paginated `GitTree` (`truncated` always `False`). Since `/src` pages by an opaque token, the first request omits `page` and each subsequent one forwards the cursor. Expensive on large repositories, and still subject to `get_tree`'s `max_depth`/`555` recursion limit.

## `get_commits_by_path`

- **No date filtering.** Same as `get_commits`: the file-history endpoint has no date filter, so `since`/`until` are rejected.
- **`ref` is effectively required.** The `/filehistory/{commit}/{path}` endpoint needs a commit in the URL, so when `ref` is omitted we spend an extra call resolving the repository's default branch.
- **Slash-containing refs are resolved first.** The `{commit}` path segment is parsed only up to the first `/` (same as `get_file_content`), so a slashed branch/tag ref is resolved to a commit hash via `_ref_to_commit` before the call.
- **N+1 to hydrate commits.** File-history entries embed only an abbreviated commit (hash + `links`, no message/author/date). We follow each entry's commit hash with a `GET .../commit/{hash}` call to get the full commit, so a page of N entries costs N+1 requests. The N hydration calls run concurrently via a thread pool.
