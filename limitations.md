<!--
    This file currently documents Cursor Origin only. The Bitbucket branch
    (jacquev6/document-for-bitbucket) introduces the same file for Bitbucket; merging the
    two is a mechanical interleave.
-->

# Actions not supported on Cursor Origin

Because there is no issue tracker:
  - `create_issue`
  - `create_issue_comment`
  - `delete_issue_comment`
  - `get_issue`
  - `get_issue_comments`
  - `update_issue`

Because there are no emoji reactions:
  - `create_issue_comment_reaction`
  - `create_issue_reaction`
  - `create_pull_request_comment_reaction`
  - `create_pull_request_reaction`
  - `create_review_comment_reaction`
  - `delete_issue_comment_reaction`
  - `delete_issue_reaction`
  - `delete_pull_request_comment_reaction`
  - `delete_pull_request_reaction`
  - `delete_review_comment_reaction`
  - `get_issue_comment_reactions`
  - `get_issue_reactions`
  - `get_pull_request_comment_reactions`
  - `get_pull_request_reactions`
  - `get_review_comment_reactions`

Because there are no diff-anchored (inline) review comments:
  - `create_review_comment`
  - `create_review_comment_file`
  - `get_review_comments`
  - `update_review_comment`

Because there are no review threads:
  - `collapse_pull_request_comment`
  - `get_thread_id_from_review_comment_unique_id`
  - `minimize_comment`
  - `resolve_review_thread`
  - `update_and_collapse_pull_request_comment`

Because the git database and the contents endpoint are read-only:
  - `create_branch`
  - `create_commit`
  - `create_git_blob`
  - `create_git_commit`
  - `create_git_tree`
  - `delete_branch`
  - `update_branch`

Because there is no archive endpoint:
  - `download_archive`
  - `get_archive_link`

Because there is no CI surface of Origin's own:
  - `download_workflow_job_log`
  - `list_workflow_jobs`
  - `list_workflow_runs`

Because there is no labels, topics, assignee, or collaborator concept:
  - `get_repository_assignees`
  - `get_repository_labels`
  - `get_repository_topics`
  - `get_repository_user_permission`
  - `list_repository_user_permissions`
  - `request_review`

Because the endpoint accepts the filter and ignores it (see "Silently ignored parameters"):
  - `get_commits_by_path`

Implemented by neither Origin nor us yet, but possible — see actions-quick-ref.md for each:
`create_review_comment_reply`, `delete_pull_request_comment`, `get_app_installation`,
`get_pull_request_diff`, `get_pull_request_review`, `get_pull_request_review_threads`,
`get_pull_request_template`.

# Consequences

## No inline review comments

This is the largest gap, and it lands squarely on Seer. Origin's pull request comments are, in the
docs' own words, "general-discussion comments, not diff-anchored review comments": the schema has no
`path`, `line`, `side`, `startLine`, or `commitId`. There is no way to attach a comment to a diff
line through the public API, and `POST .../reviews` takes a verdict and a body but no `comments[]`
array.

So a Seer review on Origin cannot be a set of line comments. It has to be a single comment (or review
body) that names the file and line in prose. `create_review` and `create_review_comment` **raise**
rather than dropping the inline comments, because a review that silently loses its findings is worse
than one that fails loudly.

Knock-on effects:
- Seer's dedupe-against-existing-comments logic keys on review comments. On Origin it will have to key
  on general comments instead.
- Nothing can be resolved or collapsed, so superseded Seer comments accumulate on the thread. Editing
  a comment in place (`PATCH /pulls/comments/{id}`, author-only) is the closest substitute, but no
  action maps to it yet.

## No emoji reactions

Same consequence as Bitbucket: Sentry cannot acknowledge that it has queued work — the 👀-on-a-new-PR
affordance is unavailable. Those code paths have to be optional.

## No commit, branch, or content writes

The entire git-write surface is Git-over-HTTPS only:
`https://x-access-token:${INSTALLATION_TOKEN}@origin.cursor.com/{owner}/{repo}.git`. There is no
endpoint to create a blob, tree, commit, ref, or branch, and none to write file contents.

For Seer this reinforces what milestone 00 already found about archives: it must **clone**, commit
locally, and push. `create_commit` is not merely unimplemented here — there is nothing to implement it
with.

## No archive download

`tarball`, `zipball`, and `archive` all 404. Materialize repositories by cloning with the installation
token. A reference implementation is at
`~/git/seer/src/seer/code_review/evals/cached_repo_manager.py`.

## No repository size, language, or topic hint

`GET /repos/{o}/{r}` returns id, name, fullName, owner, defaultBranch, cloneUrl and timestamps —
nothing else. `GitRepository.size` is therefore `0`, `description` is `None`, and `topics` is empty.
Platform detection has to weigh the tree's blob sizes instead of asking the repository, which the tree
endpoint supports well (every blob carries a `size`).

`private` is reported as `True`. Origin has no visibility field at all, and treating an unknown as
public is the error with consequences.

# Cursor Origin quirks

Corner cases discovered while implementing the provider, all verified against `sentry/nuget-trends`
unless noted. These are the behaviors that diverge from GitHub/GitLab or from what the generic action
signatures suggest.

## Silently ignored parameters

**The single most dangerous property of this API.** Origin does not reject unknown query parameters —
it ignores them and returns a normal 200 with unfiltered data. Verified:

| Parameter | Endpoint | Behavior |
|---|---|---|
| `ref` | `/commits` | ignored — the parameter is `sha`, as on GitHub |
| `since`, `until` | `/commits` | ignored — no date filtering exists |
| `path` | `/commits` | ignored — no per-file history exists |
| `name`, `branch` | `/branches` | ignored — the list cannot be filtered |
| `pageSize` | `/compare/{basehead}` | ignored — the response is not paginated |

A caller passing `?ref=<old sha>` gets a plausible-looking page of commits starting at HEAD instead.
So the provider **raises** on `since`/`until` and does not implement `get_commits_by_path`, rather
than forwarding a filter that will not be applied.

## Pagination

- Cursor-based: `pageSize` (default 30, clamped to 100) plus an opaque `pageToken`; the response
  carries `nextPageToken`, which is *absent* — not empty — on the last page.
- A page token **encodes the filters it was issued for**. `sha`, `state`, and `pageSize` are ignored
  on any request that carries one, so a filter change means restarting pagination.
- `scm.helpers.iter_all_pages` seeds its first request with `cursor="1"`, a GitHub page number, which
  Origin would reject as a malformed token with a 400. The provider drops that sentinel and requests
  the first page without a token. Real Origin tokens are long opaque strings, so the collision is
  theoretical.
- An empty collection comes back as `{}` — the collection key is omitted entirely, not sent as `[]`.
  Every mapper reads its key defensively.
- Not paginated at all: `compare`, `contents`, `git/trees`, `git/matching-refs`.

## `get_file_content` / `get_directory_contents`

- **`contents` is a query parameter.** `GET /repos/{o}/{r}/contents?path=X&ref=Y`. The documented
  `/contents/{path}` form 404s with `{"message": "Route GET:... not found"}`, which reads like an auth
  or permission failure and is not one. This is the single likeliest thing to cost an afternoon.
- **`size` is a JSON string** on this endpoint (`"2534"`), because Origin serializes 64-bit integers as
  strings. It is an `int` on `git/blobs`. The mapper coerces.
- Directory entries carry `name`, `path`, `sha`, and `type` but **no size and no content** — use
  `get_tree` when byte counts matter.
- A missing file is a plain 404, so an existence check is a status-code check.
- One endpoint serves both files and directories; the response's `type` (`file` vs `dir`) is what
  distinguishes them, so `get_file_content` raises `PathIsDirectory` and `get_directory_contents`
  raises `PathIsNotDirectory` off that field rather than off the response's shape.

## `get_tree` / `get_full_tree`

- GitHub-identical shape: `{"sha", "tree": [{"path", "mode", "type", "sha", "size"}]}`, and **every
  blob carries a `size`** (254/254 on nuget-trends). This is what makes language detection by weighted
  byte count possible despite the repository object having no size.
- **Any non-empty `recursive` value enables recursion — including the string `"false"`.** The provider
  omits the parameter entirely when not recursing.
- **No `truncated` field.** GitHub's flag has no counterpart, so `GitTree.truncated` is always `False`.
  Whether Origin silently caps very large trees is unknown; 303 entries came back whole.

## `get_branch`

- There is no `/branches/{name}` route (it 404s). `GET /branches` exists but only as an unfiltered
  list. The branch is read through `GET /git/ref/heads/{branch}` instead, which returns exactly the
  `{ref, object: {sha}}` that `GitRef` needs.

## `get_commit`

- **Two endpoints, N+1 requests.** Origin splits what GitHub returns in one response: the commit
  (`/commits/{sha}`) carries no files, and `/commits/{sha}/files` is paginated. `get_commit` walks the
  file list to exhaustion, so a large commit costs 1 + ceil(files/100) requests.
- **Zero-valued counts are omitted, not sent as `0`.** A commit with no deletions has no `deletions`
  key in `stats` or on its files. The mappers default those to `0` for files, and leave
  `Commit.additions`/`deletions` as `None` when `stats` is absent entirely (as it is on the list
  endpoint).
- Commit payloads carry only git identities — there is no account attribution — so `author_login` and
  `committer_login` are never populated.

## `compare_commits`

- **The comparison carries no commit list and no file list.** The response is exactly
  `{status, aheadBy, behindBy, baseCommit, headCommit, mergeBaseCommit}`. There is no
  `compare/{basehead}/files`, no `compare/{basehead}/commits`, no repo-level `diff` route, and no
  `includeFiles`-style parameter (all probed; all 404 or ignored).
- So `commits` is assembled by walking `/commits?sha={end_sha}` back to the merge base — one extra
  request per page — and **`diff` is always empty**. Nothing in the API reports the files changed
  between two arbitrary commits. A caller that needs them has to union per-commit `/files`, which is
  not the same thing as a diff.
- `ahead_by` and `behind_by` are always both present, so `include_behind` is a no-op (unlike GitLab,
  where it costs a request).
- The `basehead` separator must be three dots. `a..b` returns 400.

## `create_check_run` / `update_check_run`

- **A run cannot exist without a suite.** `POST /check-runs` creates both in one call, and both are
  identified by a caller-chosen `key`. The provider derives the key from `external_id`, falling back
  to `name`, so repeated calls for the same logical check converge on the same run.
- **`externalUpdatedAt` is mandatory** and is what Origin uses to order concurrent writes. The provider
  stamps it with the current time. Origin's own guidance is to use an increasing value per update and
  a fresh `externalId` per retry.
- **There is no PATCH.** An update is a re-POST with the same key, which means reading the existing run
  *and* its suite first to recover the key, name, and head sha — three requests per update.
- The check-run list endpoints take no filters, so `check_name`/`status` are applied client-side after
  the page is fetched. A page can therefore come back short or empty while later pages still hold
  matches: walk to the end rather than stopping at the first empty page. `timestamp_filter` has no
  counterpart and is ignored.
- `:batchUpsert` (1–10 runs, all-or-nothing) exists and is unused; no action maps to it.

## `create_review`

- A review is a `verdict` (`approve` / `request_changes` / `comment`) plus a body. It is **submitted
  immediately** — there is no pending/draft review flow — and it is pinned to a pull request *version*,
  not a commit, so the protocol's `commit_sha` argument is unused.
- A new `approve` or `request_changes` supersedes the caller's previous live decision on the same PR.
- Reviews can be listed but not fetched by id; pending reviews are never returned.

## `create_pull_request_comment`

- `extensions` (GitHub's Copilot-chat actions) is rejected rather than dropped. A caller that asked for
  interactive actions should not silently get a plain comment.
- Omitting a `threadId` starts a new thread; supplying one replies within it. The protocol has no
  reply-to-pull-request-comment action, so the reply form is currently unreachable.

## `mark_pull_request_ready_for_review` / `mark_pull_request_as_draft`

- `draft` is a plain field on the PR, so each is a single PATCH — no read-first, no GraphQL (contrast
  GitHub, which needs a node id and a mutation).

## Web URLs — the repository prefix is documented, the rest is a guess

**No resource in the API carries a web URL, and this is deliberate rather than an oversight.** An
audit of all 144 schemas in [`openapi.yaml`](https://cursor.com/docs/api/origin/openapi.yaml) finds
exactly six URL-shaped fields: `App.webhookUrl`, `Repo.cloneUrl`, and four `detailsUrl` (on
`CheckRun`/`CheckSuite` and their inputs). There is no `htmlUrl`, `html_url`, `webUrl`, `url`, or
`_links` anywhere. Two schema descriptions say so outright — `CommitParent` is "Only the SHA is
exposed (URLs omitted)", and the compare summary is "without nested commit lists, file diffs, or URL
fields".

**Webhook payloads carry none either.** `PullRequestWebhookPayload` is
`{pullRequest, repository}`; the comment, push, and check-run payloads are the same story. Anything
consuming a webhook has to assemble the link from `repository.owner.slug`, `repository.name`, and
`pullRequest.number` — exactly what this provider does.

What *is* documented, in the product docs rather than the API reference: the codebase name is
"the `{owner}` in `https://cursor.com/codebase/{owner}/{repo}`". So the repository-level prefix the
provider builds on is citable.

**The per-resource suffixes are not.** `/pull/{n}`, `/commit/{sha}`, and `/blob/{sha}/{path}` are
GitHub-shaped guesses; nothing in the docs, the spec, the changelog, or search results pins them
down, and everything under `/codebase/` redirects to login whether the path is real or nonsense, so
an unauthenticated probe cannot distinguish them either. **Confirm them in the web UI before anyone
demos a link.** They surface as `get_file_url`, `get_commit_url`, `get_commits_url`,
`get_pull_request_url`, and the `html_url` on every mapped `PullRequest` and `Review`.

One consequence for check runs: `CheckRun.detailsUrl` is **caller-supplied** (the input schema is
writable, the response field is `readOnly` and just echoes it). It is meant to point at the CI
system's own page, not at Origin. Since `create_check_run`'s protocol signature has no URL argument,
we never set it, so `CheckRun.html_url` is always empty. If Seer wants its check runs to link back to
Sentry, that argument has to be added to the protocol first.

## Rate limits

3000 points per minute per installation token, reported in `x-ratelimit-{limit,remaining,used,reset,resource}`
with `resource: core`. Most reads cost 1 point; writes cost 5, as do `GetCommit`, `ListCommitFiles`,
and `ListPullRequestFiles`. `GET /rate_limit` is free. The `update_check_run` read-read-write dance
therefore costs 7 points, and `get_commit` on a large commit costs 5 per file page.

## Conditional requests

Single-resource reads carry an ETag — the object's sha — and Origin honors `If-None-Match` with a 304
(verified). List endpoints send no ETag, and `Last-Modified` is never sent.

## Auth

- The app JWT is **EdDSA over Ed25519**, not RS256; PyJWT needs a key object rather than a PEM string.
  It requires `aud: origin-apps` and a `kid` header carrying the app id.
- Installation tokens expire in **under 15 minutes** — a quarter of GitHub's hour. Long-running work
  must refresh mid-flight.
- `/app*` endpoints require the app JWT and reject an installation token with
  `403 "This Origin endpoint requires an authenticated app principal"`. That is why
  `get_authenticated_actor` passes `credentials_set="application"`.

## Repositories mirrored in from GitHub

Origin apps **cannot reach repositories that Origin mirrors in from GitHub** — they are excluded from
installations, tokens, and webhooks — and merging is rejected on mirrored repos. `sentry/nuget-trends`
is a native Origin repository, so nothing here exercises that path.

## API stability

The API self-reports as `v1alpha1` and the docs label it Alpha / Early Beta, "subject to change".
Everything above was true on 2026-08-17.
