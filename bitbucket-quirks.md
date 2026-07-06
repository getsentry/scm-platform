# Bitbucket quirks

Corner cases and limitations discovered while implementing the Bitbucket provider.
These are behaviors that diverge from GitHub/GitLab or from what the generic action signatures suggest.

## `update_pull_request`

- **Cannot reopen a pull request.** Bitbucket has no API to reopen a declined  pull request, so `state="open"` raises `ResourceBadRequest`.
Closing works via the separate `POST .../pullrequests/{id}/decline` endpoint (there is no `state` field on the PUT body).
