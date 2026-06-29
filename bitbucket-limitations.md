# Actions not supported on Bitbucket

Because the issue tracker is deprecated:
  - `create_issue`
  - `create_issue_comment`
  - `delete_issue_comment`
  - `get_issue`
  - `get_issue_comments`
  - `get_repository_labels`
  - `update_issue`

Because there is no emoji reactions:
  - `create_issue_comment_reaction`
  - `create_issue_reaction`
  - `create_pull_request_comment_reaction`
  - `create_pull_request_reaction`
  - `delete_issue_comment_reaction`
  - `delete_issue_reaction`
  - `delete_pull_request_comment_reaction`
  - `delete_pull_request_reaction`
  - `get_issue_comment_reactions`
  - `get_issue_reactions`
  - `get_pull_request_comment_reactions`
  - `get_pull_request_reactions`

Because there are no repository topics:
  - `get_repository_topics`

# Consequences

## Deprecated issue tracker

I've not found any use (of the SCM action) in Seer or Sentry.
There are definitely some code paths using GitHub/GitLab issues, but not yet migrated to SCM.

Also note that Sentry *is using* the now-deprecated Bitbucket issue tracker.

## No Emoji reactions

Sentry cannot communicate that it's doing something, or has queued a request to do something.

This is used, at least, when a PR is opened, or a "`@sentry` review" comment is posted.

*Important* This will limit how the user perceive Sentry's reactivity.

Implementation: we'll have to make all these code path optional.

## No repository topics

Used in Seer to pass them as context to an LLM.
My opinion is we can live without them.

Implementation: current code will emit a warning log, and use an empty list.
