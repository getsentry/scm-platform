# Bitbucket quirks

Corner cases and limitations discovered while implementing the Bitbucket provider.
These are behaviors that diverge from GitHub/GitLab or from what the generic action signatures suggest.

## `update_pull_request`

- **Cannot reopen a pull request.** Bitbucket has no API to reopen a declined  pull request, so `state="open"` raises `ResourceBadRequest`.
Closing works via the separate `POST .../pullrequests/{id}/decline` endpoint (there is no `state` field on the PUT body).

## `get_commits`

- **No date filtering.** Bitbucket's commits endpoint supports neither `since`/`until` params nor `q` date filtering, so passing `since` or `until` raises `ResourceBadRequest` rather than silently returning unfiltered commits.

## `get_file_content`

- **Blob SHA computed locally.** Bitbucket's `/src` endpoint returns raw bytes with no git blob id, so we recompute it as `sha1("blob <len>\0" + content)` to match the blob SHA GitHub/GitLab report. Content is base64-encoded and `size` is derived from the response's byte length.

## `get_commits_by_path`

- **No date filtering.** Same as `get_commits`: the file-history endpoint has no date filter, so `since`/`until` are rejected.
- **`ref` is effectively required.** The `/filehistory/{commit}/{path}` endpoint needs a commit in the URL, so when `ref` is omitted we spend an extra call resolving the repository's default branch.
- **N+1 to hydrate commits.** File-history entries embed only an abbreviated commit (hash + `links`, no message/author/date). We follow each entry's commit hash with a `GET .../commit/{hash}` call to get the full commit, so a page of N entries costs N+1 requests. The N hydration calls run concurrently via a thread pool.
