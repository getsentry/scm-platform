<!--
    This file currently documents Cursor Origin only. The Bitbucket branch
    (jacquev6/document-for-bitbucket) introduces the same file for Bitbucket; merging the
    two is a mechanical interleave.
-->

# Scope: built against the published API, not against the product

Origin's API self-reports as `v1alpha1` and its docs label it Alpha / Early Beta, "subject to change".
This provider is deliberately built to **what the [OpenAPI document](https://cursor.com/docs/api/origin/openapi.yaml)
publishes and we could verify on the wire** — nothing else.

Two consequences worth being explicit about, because both are choices rather than accidents:

1. **A feature in the web UI is not a feature we implement.** Origin's UI has reactions; its API does
   not expose them. We do not reverse-engineer around that — an undocumented endpoint found by probing
   is one Cursor never promised, can change without notice, and would fail in production long after
   whoever added it moved on.
2. **Every gap below is provisional.** The list describes the API as of **2026-08-17**, not a permanent
   verdict on Origin. The section "Revisit when Origin supports it" collects the ones we would
   implement the moment they ship, so the next person reads them as pending rather than settled.

Where a gap has a cheap, honest partial answer, we take it and say so — see "Degraded inline review
comments". Where it does not, the action stays unimplemented and the facade hides it.

# Revisit when Origin supports it

In rough order of what would buy the most, and what to watch for:

| Gap | Watch for | Unblocks |
|---|---|---|
| **Reactions** | any reaction field or route on pull request comments | Sentry's 👀 "request queued" acknowledgement — the most visible loss, and the smallest ask |
| **Inline review comments** | `path`/`line`/`side` on the comment schema, or `comments[]` on `POST /reviews` | Seer reviews as real line comments instead of located general ones |
| **Review threads & resolve** | a thread resource, or `resolved` on a comment | retiring superseded Seer findings instead of editing them in place |
| **Content / commit / branch writes** | any `POST` under `contents`, `git/*`, or `branches` | `create_commit` and friends; today Seer must clone and push |
| **`compare` file list** | `files[]` on the compare response | `compare_commits().diff`, which is empty today |
| **Commit date and path filters** | `since`/`until`/`path` actually filtering rather than being ignored | `get_commits(since=…)` and `get_commits_by_path`, which currently raise |
| **Reviewer operations** | any reviewer add/remove route (webhook-only today) | `request_review` |
| **Archive download** | a `tarball`/`zipball` route | `get_archive_link`; Seer could stop cloning |

Anything on this list that starts working is a small, contained change here — the mapping tables and
docstrings already name the endpoint each one would use.

# Actions not supported on Cursor Origin

Because there is no issue tracker:
  - `create_issue`
  - `create_issue_comment`
  - `delete_issue_comment`
  - `get_issue`
  - `get_issue_comments`
  - `update_issue`

Because the API exposes no emoji reactions — **the web UI does have them**, so this is a gap in the
partner API rather than a missing product feature (see "Reactions exist in the UI but not the API"):
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
  - `get_review_comments`

  Implemented but **degraded** — they post a general comment stating the file and line
  rather than anchoring to the diff (see "Degraded inline review comments"):
  - `create_review`
  - `create_review_comment`
  - `create_review_comment_file`

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
`delete_pull_request_comment`, `get_pull_request_diff`, `get_pull_request_review`,
`get_pull_request_review_threads`, `get_pull_request_template`.

`get_app_installation` **is** implemented, and it is load-bearing for Seer rather than optional:
`check_repo_access` calls it to decide whether Seer may open a pull request, and treats a provider
that does not implement it as "no write access" — so the whole PR path fails closed, with a message
about permissions rather than about a missing method.

# Consequences

## No inline review comments

This is the largest gap, and it lands squarely on Seer. Origin's pull request comments are, in the
docs' own words, "general-discussion comments, not diff-anchored review comments": the schema has no
`path`, `line`, `side`, `startLine`, or `commitId`. There is no way to attach a comment to a diff
line through the public API, and `POST .../reviews` takes a verdict and a body but no `comments[]`
array.

So a Seer review on Origin cannot be a set of line comments.

### Degraded inline review comments

Rather than refusing, the provider degrades: an inline comment becomes a general-discussion comment
whose first line states where it belongs.

```
**`src/NuGetTrends.Web/Startup.cs`** line 42

The finding text, unchanged.
```

`create_review_comment`, `create_review_comment_file`, and `create_review` (which posts its verdict
first, then each finding) all go through this path. The header is built by
`format_comment_location`; a base-side line is labeled `line 17 (before)` so a reader who cannot find
it in the current file knows why, and a range renders as `lines 10-20`.

It is deliberately **plain text, not a link.** The blob URL shape is still unverified, and a dead link
is worse than a path the reader can search for. Once `/blob/{sha}/{path}#L{n}` is confirmed, this is
the one place to change.

What the caller gets back is honest about the compromise. The returned `ReviewComment` echoes the
`file_path` and `line` that were *asked for*, not an anchor Origin is holding — nothing in Origin
knows the comment belongs to a diff position, so it will not move with the diff, will not appear in
the file view, and cannot be resolved.

`create_review` is **not atomic**: the verdict is one request and each finding another. The verdict
goes first on purpose — if it went last, a partial failure would leave findings posted under no
review at all, which reads as an unattributed drive-by. This way the review exists and the gap is in
its detail.

### What still works natively

- **Threaded replies.** `create_review_comment_reply` works properly: Origin threads by `threadId`,
  which the provider resolves from the parent comment id (two requests). Verified live — the reply
  lands in the parent's thread.
- **Editing in place.** `update_review_comment` maps to `PATCH /pulls/comments/{id}` (author-only).
  With no resolve or collapse, editing is the only way to retract a superseded finding rather than
  leave the stale text sitting there.

### Knock-on effects

- Seer's dedupe-against-existing-comments logic keys on review comments. On Origin it has to key on
  general comments instead — `get_pull_request_comments` plus the bot login (`author.app.slug`) is
  enough to find its own.
- Nothing can be resolved or collapsed, so superseded comments accumulate unless edited in place.
- `get_review_comments` (listing the comments of a given review) has no counterpart at all: Origin
  does not associate comments with a review.

## Reactions exist in the UI but not the API

Origin's web UI **does** support reactions — you can leave a 👍 on a pull request comment. The partner
API does not expose them, which is worth stating precisely because the two are easy to conflate:

- Fetching a comment that visibly *has* a reaction returns only
  `{id, thread, body, author, createdAt, updatedAt}` — no reactions field, no count, no rollup.
- Every plausible reaction route (`/pulls/comments/{id}/reactions`,
  `/pulls/{n}/comments/{id}/reactions`, `/pulls/{n}/reactions`, `/reactions`) answers
  `"Route GET:… not found"`. That is the router, not authorization — routing happens first — so the
  paths genuinely do not exist rather than being hidden from our token.
- The OpenAPI document contains no occurrence of reaction, emoji, thumbs, award, or vote.

So this is a **gap in the partner API, not a missing product feature**, and a good candidate to raise
with Cursor: it needs a read (rollup or list) and a create, on pull request comments. Until then the
consequence is the same as Bitbucket's: Sentry cannot acknowledge that it has queued work — the
👀-on-a-new-PR affordance is unavailable, and those code paths have to be optional.

Caveat on the evidence: everything above was probed with an *installation* token, the only credential
this provider uses. The route-not-found responses are authorization-independent, so the endpoints are
absent for every caller — but if Cursor ever adds them under user-scoped auth only, an app would still
be unable to reach them.

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

`CURSOR_ORIGIN_WEB_BASE_URL` here is the **codebase root** — `https://cursor.com/codebase`, with the
repository directly beneath it — which is the same value and meaning as the constant of that name in
`sentry/integrations/cursor_origin/constants.py`. Keep the two in step: whatever Sentry stores on the
repository arrives here as `web_base_url`, so a base that already ends in `/codebase` combined with a
provider that appends its own would yield `/codebase/codebase/…`.

**The per-resource suffixes are undocumented, and were settled by looking.** Nothing in the docs, the
spec, the changelog, or search results pins them down, and everything under `/codebase/` redirects to
login whether the path is real or nonsense, so an unauthenticated probe cannot distinguish them
either. Status:

| Builder | Suffix | |
|---|---|---|
| `get_pull_request_url` (and `PullRequest.html_url`, `Review.html_url`) | `/pull/{n}` | ✅ confirmed; the plural `/pulls/{n}` is a **dead link** |
| `get_file_url` | `/blob/{sha}/{path}` and `/blob/{branch}/{path}` | ✅ confirmed, both refs |
| `get_file_url` | `#L{start}-L{end}` | ✅ confirmed — the anchor works |
| `get_commit_url` | `/commit/{sha}` | unconfirmed |
| `get_commits_url` | `/commits/{sha}`, plus `/{path}` when scoped to a file | unconfirmed |
| *(no protocol method)* | `/pull/{n}#discussion-{commentId}` | ✅ confirmed — a comment permalink |

`get_file_url` is fully settled: the `/blob/` segment, a branch *or* a 40-character sha in the ref
position, and the line anchor were each checked in a logged-in browser. That last one mattered most —
Seer uses it to point at a specific line, and an anchor the UI did not understand would have degraded
quietly to top-of-file rather than erroring.

The two commit shapes are the only ones left, and nothing depends on them urgently: `get_commit_url`
feeds Sentry's commit links, `get_commits_url` its history links.

Note that **curl cannot settle any of these.** Every path under `/codebase/` answers 307 to
`cursor.com/api/auth/login` — including deliberately malformed ones — because Origin authenticates
before it routes. A valid URL and a nonsense URL are indistinguishable unauthenticated. Only a
logged-in browser decides, which is why these rows moved slowly.

That last row is a freebie worth remembering: a comment anchors as `#discussion-{commentId}`, and the
comment id is exactly what `create_pull_request_comment` and the degraded review comments return. There
is no protocol method for a comment URL, so nothing uses it yet, but it is the one link shape that
could point a reader straight at a specific finding.

The line anchor on `get_file_url` is the one worth checking hardest: it is how Seer points at a
specific line, and an anchor the UI does not understand degrades quietly to "top of file" rather than
failing.

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
Everything above was true on 2026-08-17 — see "Scope" at the top for how that shapes what is
implemented, and "Revisit when Origin supports it" for what to re-check when the API moves.

A concrete re-check list for whoever revisits this: the endpoints whose *behavior* we depend on and
that could change without breaking loudly are the ignored query parameters (a `since` that starts
working would silently begin filtering where we currently raise), `recursive` accepting any non-empty
value, `size` being a string on `contents` but an int on `git/blobs`, and the absence of a `truncated`
flag on trees. Each of those has a test naming it.
