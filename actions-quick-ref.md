# Introduction

"Actions" below refer to free functions in `src/scm/actions.py`.
They correspond to protocol abilities defined in `src/scm/types.py` and implemented in `src/scm/providers`.

This file lists the API they call.
For more details, refer to the implementation.

The note "*not implemented*" makes no assumption about our ability to do it.
It just show that we've not done it yet. On the contrary, *not supported* means that this provider does not have a close enough functionality.

<!--
    Note to maintainers:
    - Actions are kept in alphabetical order.
    - There may be errors in links to API docs. Please fix them if you find any.
    - Keep this file terse.
-->

# Actions

## `collapse_pull_request_comment`

GitHub: [`resolveReviewThread`](https://docs.github.com/en/graphql/reference/pulls#mutation-resolvereviewthread) or [`minimizeComment`](https://docs.github.com/en/graphql/reference/issues#mutation-minimizecomment)

GitLab: [PUT /projects/:id/merge_requests/:merge_request_iid/discussions/:discussion_id](https://docs.gitlab.com/api/discussions/#resolve-a-merge-request-thread)

Bitbucket: @todo Use [POST /repositories/{workspace}/{repo_slug}/pullrequests/{pull_request_id}/comments/{comment_id}/resolve](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-comments-comment-id-resolve-post)

## `compare_commits`

GitHub: [GET /repos/{owner}/{repo}/compare/{start_sha}...{end_sha}](https://docs.github.com/en/rest/commits/commits#compare-two-commits)

GitLab: [GET /projects/:id/repository/compare](https://docs.gitlab.com/api/repositories/#compare-branches-tags-or-commits)

Bitbucket:
- [GET /repositories/{workspace}/{repo_slug}/commits/{revision}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-commits/#api-repositories-workspace-repo-slug-commits-revision-get)
- [GET /repositories/{workspace}/{repo_slug}/diffstat/{spec}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-commits/#api-repositories-workspace-repo-slug-diffstat-spec-get)

## `create_branch`

GitHub: [POST /repos/{owner}/{repo}/git/refs](https://docs.github.com/en/rest/git/refs#create-a-reference)

GitLab: [POST /projects/:id/repository/branches](https://docs.gitlab.com/api/branches/#create-repository-branch)

Bitbucket: [POST /repositories/{workspace}/{repo_slug}/refs/branches](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-refs/#api-repositories-workspace-repo-slug-refs-branches-post)

## `create_check_run`

GitHub: [POST /repos/{owner}/{repo}/check-runs](https://docs.github.com/en/rest/checks/runs#create-a-check-run)

GitLab: [POST /projects/:id/statuses/:sha](https://docs.gitlab.com/api/commits/#set-commit-pipeline-status)

Bitbucket: @todo Use [POST /repositories/{workspace}/{repo_slug}/commit/{commit}/statuses/build](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-commit-statuses/#api-repositories-workspace-repo-slug-commit-commit-statuses-build-post)

## `create_commit`

GitHub:
- [POST /repos/{owner}/{repo}/git/blobs](https://docs.github.com/en/rest/git/blobs#create-a-blob)
- [GET /repos/{owner}/{repo}/contents/{path}](https://docs.github.com/en/rest/repos/contents#get-repository-content)
- [GET /repos/{owner}/{repo}/git/commits/{sha}](https://docs.github.com/en/rest/git/commits#get-a-commit-object)
- [POST /repos/{owner}/{repo}/git/trees](https://docs.github.com/en/rest/git/trees#create-a-tree)
- [POST /repos/{owner}/{repo}/git/commits](https://docs.github.com/en/rest/git/commits#create-a-commit)
- [POST /repos/{owner}/{repo}/git/refs](https://docs.github.com/en/rest/git/refs#create-a-reference) or [PATCH /repos/{owner}/{repo}/git/refs/heads/{branch}](https://docs.github.com/en/rest/git/refs#update-a-reference)

GitLab: [POST /projects/:id/repository/commits](https://docs.gitlab.com/api/commits/#create-a-commit)

Bitbucket: @todo Use [POST /repositories/{workspace}/{repo_slug}/src](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-source/#api-repositories-workspace-repo-slug-src-post)

## `create_git_blob`

GitHub: [POST /repos/{owner}/{repo}/git/blobs](https://docs.github.com/en/rest/git/blobs#create-a-blob)

GitLab: *not implemented*

Bitbucket: *not implemented*

## `create_git_commit`

GitHub: [POST /repos/{owner}/{repo}/git/commits](https://docs.github.com/en/rest/git/commits#create-a-commit)

GitLab: *not implemented*

Bitbucket: *not implemented*

## `create_git_tree`

GitHub: [POST /repos/{owner}/{repo}/git/trees](https://docs.github.com/en/rest/git/trees#create-a-tree)

GitLab: *not implemented*

Bitbucket: *not implemented*

## `create_issue`

GitHub: [POST /repos/{owner}/{repo}/issues](https://docs.github.com/en/rest/issues/issues#create-an-issue)

GitLab: [POST /projects/:id/issues](https://docs.gitlab.com/api/issues/#create-an-issue)

Bitbucket: *not supported* (issue tracker deprecated)

## `create_issue_comment`

GitHub: [POST /repos/{owner}/{repo}/issues/{issue_number}/comments](https://docs.github.com/en/rest/issues/comments#create-an-issue-comment)

GitLab: [POST /projects/:id/issues/:issue_iid/notes](https://docs.gitlab.com/api/notes/#create-an-issue-note)

Bitbucket: *not supported* (issue tracker deprecated)

## `create_issue_comment_reaction`

GitHub: [POST /repos/{owner}/{repo}/issues/comments/{comment_id}/reactions](https://docs.github.com/en/rest/reactions/reactions#create-reaction-for-an-issue-comment)

GitLab: [POST /projects/:id/issues/:issue_iid/notes/:note_id/award_emoji](https://docs.gitlab.com/api/emoji_reactions/#add-an-emoji-reaction-to-a-comment)

Bitbucket: *not supported* (no emoji reactions)

## `create_issue_reaction`

GitHub: [POST /repos/{owner}/{repo}/issues/{issue_number}/reactions](https://docs.github.com/en/rest/reactions/reactions#create-reaction-for-an-issue)

GitLab: [POST /projects/:id/issues/:issue_iid/award_emoji](https://docs.gitlab.com/api/emoji_reactions/#add-an-emoji-reaction-to-a-resource)

Bitbucket: *not supported* (no emoji reactions)

## `create_pull_request`

GitHub: [POST /repos/{owner}/{repo}/pulls](https://docs.github.com/en/rest/pulls/pulls#create-a-pull-request)

GitLab: [POST /projects/:id/merge_requests](https://docs.gitlab.com/api/merge_requests/#create-a-merge-request)

Bitbucket: [POST /repositories/{workspace}/{repo_slug}/pullrequests](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-post)

## `create_pull_request_comment`

GitHub: [POST /repos/{owner}/{repo}/issues/{pull_number}/comments](https://docs.github.com/en/rest/issues/comments#create-an-issue-comment)

GitLab: [POST /projects/:id/merge_requests/:merge_request_iid/notes](https://docs.gitlab.com/api/notes/#create-a-merge-request-note)

Bitbucket: [POST /repositories/{workspace}/{repo_slug}/pullrequests/{pull_request_id}/comments](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-comments-post)

## `create_pull_request_comment_reaction`

GitHub: [POST /repos/{owner}/{repo}/issues/comments/{comment_id}/reactions](https://docs.github.com/en/rest/reactions/reactions#create-reaction-for-an-issue-comment)

GitLab: [POST /projects/:id/merge_requests/:merge_request_iid/notes/:note_id/award_emoji](https://docs.gitlab.com/api/emoji_reactions/#add-an-emoji-reaction-to-a-comment)

Bitbucket: *not supported* (no emoji reactions)

## `create_pull_request_draft`

GitHub: [POST /repos/{owner}/{repo}/pulls](https://docs.github.com/en/rest/pulls/pulls#create-a-pull-request) with `draft: true`

GitLab: [POST /projects/:id/merge_requests](https://docs.gitlab.com/api/merge_requests/#create-a-merge-request) with `Draft:` title prefix

Bitbucket: @todo Use [POST /repositories/{workspace}/{repo_slug}/pullrequests](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-post) with `draft: true`

## `create_pull_request_reaction`

GitHub: [POST /repos/{owner}/{repo}/issues/{issue_number}/reactions](https://docs.github.com/en/rest/reactions/reactions#create-reaction-for-an-issue)

GitLab: [POST /projects/:id/merge_requests/:merge_request_iid/award_emoji](https://docs.gitlab.com/api/emoji_reactions/#add-an-emoji-reaction-to-a-resource)

Bitbucket: *not supported* (no emoji reactions)

## `create_review`

GitHub: [POST /repos/{owner}/{repo}/pulls/{pull_number}/reviews](https://docs.github.com/en/rest/pulls/reviews#create-a-review-for-a-pull-request)

GitLab:
- [GET /projects/:id/merge_requests/:merge_request_iid/versions](https://docs.gitlab.com/api/merge_requests/#retrieve-merge-request-diff-versions)
- [POST /projects/:id/merge_requests/:merge_request_iid/discussions](https://docs.gitlab.com/api/discussions/#create-a-merge-request-thread)
- [POST /projects/:id/merge_requests/:merge_request_iid/notes](https://docs.gitlab.com/api/notes/#create-a-merge-request-note)
- [POST /projects/:id/merge_requests/:merge_request_iid/approve](https://docs.gitlab.com/api/merge_request_approvals/#approve-merge-request)

Bitbucket: @todo Use:
- [POST /repositories/{workspace}/{repo_slug}/pullrequests/{pull_request_id}/comments](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-comments-post) per inline comment (sets `inline`) and the review body
- [POST /repositories/{workspace}/{repo_slug}/pullrequests/{pull_request_id}/approve](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-approve-post) when the event is `approve`

## `create_review_comment_file`

GitHub: [POST /repos/{owner}/{repo}/pulls/{pull_number}/comments](https://docs.github.com/en/rest/pulls/comments#create-a-review-comment-for-a-pull-request)

GitLab:
- [GET /projects/:id/merge_requests/:merge_request_iid/versions](https://docs.gitlab.com/api/merge_requests/#retrieve-merge-request-diff-versions)
- [POST /projects/:id/merge_requests/:merge_request_iid/discussions](https://docs.gitlab.com/api/discussions/#create-a-merge-request-thread)

Bitbucket: @todo Use [POST /repositories/{workspace}/{repo_slug}/pullrequests/{pull_request_id}/comments](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-comments-post) (sets `inline`)

## `create_review_comment_line`

GitHub: [POST /repos/{owner}/{repo}/pulls/{pull_number}/comments](https://docs.github.com/en/rest/pulls/comments#create-a-review-comment-for-a-pull-request)

GitLab:
- [GET /projects/:id/merge_requests/:merge_request_iid/versions](https://docs.gitlab.com/api/merge_requests/#retrieve-merge-request-diff-versions)
- [POST /projects/:id/merge_requests/:merge_request_iid/discussions](https://docs.gitlab.com/api/discussions/#create-a-merge-request-thread)

Bitbucket: @todo Use [POST /repositories/{workspace}/{repo_slug}/pullrequests/{pull_request_id}/comments](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-comments-post) (sets `inline`)

## `create_review_comment_multiline`

GitHub: [POST /repos/{owner}/{repo}/pulls/{pull_number}/comments](https://docs.github.com/en/rest/pulls/comments#create-a-review-comment-for-a-pull-request)

GitLab:
- [GET /projects/:id/merge_requests/:merge_request_iid/versions](https://docs.gitlab.com/api/merge_requests/#retrieve-merge-request-diff-versions)
- [POST /projects/:id/merge_requests/:merge_request_iid/discussions](https://docs.gitlab.com/api/discussions/#create-a-merge-request-thread)

Bitbucket: @todo Use [POST /repositories/{workspace}/{repo_slug}/pullrequests/{pull_request_id}/comments](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-comments-post) (sets `inline`)

## `create_review_comment_reply`

GitHub: [POST /repos/{owner}/{repo}/pulls/{pull_number}/comments](https://docs.github.com/en/rest/pulls/comments#create-a-review-comment-for-a-pull-request)

GitLab: [POST /projects/:id/merge_requests/:merge_request_iid/discussions/:discussion_id/notes](https://docs.gitlab.com/api/discussions/#add-note-to-a-merge-request-thread)

Bitbucket: @todo Use [POST /repositories/{workspace}/{repo_slug}/pullrequests/{pull_request_id}/comments](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-comments-post) (reply sets `parent`)

## `delete_branch`

GitHub: [DELETE /repos/{owner}/{repo}/git/refs/heads/{branch}](https://docs.github.com/en/rest/git/refs#delete-a-reference)

GitLab: [DELETE /projects/:id/repository/branches/:branch](https://docs.gitlab.com/api/branches/#delete-repository-branch)

Bitbucket: [DELETE /repositories/{workspace}/{repo_slug}/refs/branches/{name}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-refs/#api-repositories-workspace-repo-slug-refs-branches-name-delete)

## `delete_issue_comment`

GitHub: [DELETE /repos/{owner}/{repo}/issues/comments/{comment_id}](https://docs.github.com/en/rest/issues/comments#delete-an-issue-comment)

GitLab: [DELETE /projects/:id/issues/:issue_iid/notes/:note_id](https://docs.gitlab.com/api/notes/#delete-an-issue-note)

Bitbucket: *not supported* (issue tracker deprecated)

## `delete_issue_comment_reaction`

GitHub: [DELETE /repos/{owner}/{repo}/issues/comments/{comment_id}/reactions/{reaction_id}](https://docs.github.com/en/rest/reactions/reactions#delete-an-issue-comment-reaction)

GitLab: [DELETE /projects/:id/issues/:issue_iid/notes/:note_id/award_emoji/:award_id](https://docs.gitlab.com/api/emoji_reactions/#delete-an-emoji-reaction-from-a-comment)

Bitbucket: *not supported* (no emoji reactions)

## `delete_issue_reaction`

GitHub: [DELETE /repos/{owner}/{repo}/issues/{issue_number}/reactions/{reaction_id}](https://docs.github.com/en/rest/reactions/reactions#delete-an-issue-reaction)

GitLab: [DELETE /projects/:id/issues/:issue_iid/award_emoji/:award_id](https://docs.gitlab.com/api/emoji_reactions/#delete-an-emoji-reaction-from-a-resource)

Bitbucket: *not supported* (no emoji reactions)

## `delete_pull_request_comment`

GitHub: [DELETE /repos/{owner}/{repo}/issues/comments/{comment_id}](https://docs.github.com/en/rest/issues/comments#delete-an-issue-comment)

GitLab: [DELETE /projects/:id/merge_requests/:merge_request_iid/notes/:note_id](https://docs.gitlab.com/api/notes/#delete-a-merge-request-note)

Bitbucket: [DELETE /repositories/{workspace}/{repo_slug}/pullrequests/{pull_request_id}/comments/{comment_id}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-comments-comment-id-delete)

## `delete_pull_request_comment_reaction`

GitHub: [DELETE /repos/{owner}/{repo}/issues/comments/{comment_id}/reactions/{reaction_id}](https://docs.github.com/en/rest/reactions/reactions#delete-an-issue-comment-reaction)

GitLab: [DELETE /projects/:id/merge_requests/:merge_request_iid/notes/:note_id/award_emoji/:award_id](https://docs.gitlab.com/api/emoji_reactions/#delete-an-emoji-reaction-from-a-comment)

Bitbucket: *not supported* (no emoji reactions)

## `delete_pull_request_reaction`

GitHub: [DELETE /repos/{owner}/{repo}/issues/{issue_number}/reactions/{reaction_id}](https://docs.github.com/en/rest/reactions/reactions#delete-an-issue-reaction)

GitLab: [DELETE /projects/:id/merge_requests/:merge_request_iid/award_emoji/:award_id](https://docs.gitlab.com/api/emoji_reactions/#delete-an-emoji-reaction-from-a-resource)

Bitbucket: *not supported* (no emoji reactions)

## `download_archive`

GitHub: [GET /repos/{owner}/{repo}/tarball/{ref}](https://docs.github.com/en/rest/repos/contents#download-a-repository-archive-tar) or [GET /repos/{owner}/{repo}/zipball/{ref}](https://docs.github.com/en/rest/repos/contents#download-a-repository-archive-zip)

GitLab: [GET /projects/:id/repository/archive{format}](https://docs.gitlab.com/api/repositories/#retrieve-file-archive-from-a-repository)

Bitbucket: @todo Use [GET https://bitbucket.org/<workspace-id>/<repository-slug>/get/<branch>.<zip|gz|bz2>](https://support.atlassian.com/bitbucket-cloud/kb/how-to-download-repositories-using-the-api/)

## `download_workflow_job_log`

GitHub: [GET /repos/{owner}/{repo}/actions/jobs/{job_id}/logs](https://docs.github.com/en/rest/actions/workflow-jobs#download-job-logs-for-a-workflow-run)

GitLab: *not implemented*

Bitbucket: *not implemented*

## `get_app_installation`

GitHub: [GET /repos/{owner}/{repo}/installation](https://docs.github.com/en/rest/apps/apps#get-a-repository-installation-for-the-authenticated-app)

GitLab: [GET /projects/:id](https://docs.gitlab.com/api/projects/#retrieve-a-project)

Bitbucket: @todo Use [GET /user/workspaces/{workspace}/permissions/repositories](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-repositories/#api-user-workspaces-workspace-permissions-repositories-get) (current user's permission, filtered by repo; no check-run permission concept)

## `get_archive_link`

GitHub: [GET /repos/{owner}/{repo}/tarball/{ref}](https://docs.github.com/en/rest/repos/contents#download-a-repository-archive-tar) or [GET /repos/{owner}/{repo}/zipball/{ref}](https://docs.github.com/en/rest/repos/contents#download-a-repository-archive-zip) with `allow_redirects=False`

GitLab: *not implemented*

Bitbucket: *not implemented*

## `get_authenticated_actor`

GitHub: [GET /app](https://docs.github.com/en/rest/apps/apps#get-the-authenticated-app) then [GET /users/{app_slug}[bot]](https://docs.github.com/en/rest/users/users#get-a-user)

GitLab: [GET /user](https://docs.gitlab.com/api/users/#retrieve-the-current-user)

Bitbucket: @todo Use [GET /user](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-users/#api-user-get)

## `get_branch`

GitHub: [GET /repos/{owner}/{repo}/branches/{branch}](https://docs.github.com/en/rest/branches/branches#get-a-branch)

GitLab: [GET /projects/:id/repository/branches/:branch](https://docs.gitlab.com/api/branches/#retrieve-a-repository-branch)

Bitbucket: [GET /repositories/{workspace}/{repo_slug}/refs/branches/{name}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-refs/#api-repositories-workspace-repo-slug-refs-branches-name-get)

## `get_check_run`

GitHub: [GET /repos/{owner}/{repo}/check-runs/{check_run_id}](https://docs.github.com/en/rest/checks/runs#get-a-check-run)

GitLab: [GET /projects/:id/repository/commits/:sha/statuses](https://docs.gitlab.com/api/commits/#list-commit-statuses)

Bitbucket: @todo Use [GET /repositories/{workspace}/{repo_slug}/commit/{commit}/statuses/build/{key}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-commit-statuses/#api-repositories-workspace-repo-slug-commit-commit-statuses-build-key-get)

## `get_commit`

GitHub: [GET /repos/{owner}/{repo}/commits/{sha}](https://docs.github.com/en/rest/commits/commits#get-a-commit)

GitLab: [GET /projects/:id/repository/commits/:sha](https://docs.gitlab.com/api/commits/#retrieve-a-commit)

Bitbucket: [GET /repositories/{workspace}/{repo_slug}/commit/{commit}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-commits/#api-repositories-workspace-repo-slug-commit-commit-get)

## `get_commit_changes`

GitHub: [GET /repos/{owner}/{repo}/commits/{sha}](https://docs.github.com/en/rest/commits/commits#get-a-commit)

GitLab: [GET /projects/:id/repository/commits/:sha/diff](https://docs.gitlab.com/api/commits/#retrieve-commit-diff)

Bitbucket: @todo Use [GET /repositories/{workspace}/{repo_slug}/diffstat/{spec}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-commits/#api-repositories-workspace-repo-slug-diffstat-spec-get)

## `get_commit_url`

GitHub: return {web_base}/{owner}/{repo}/commit/{commit_sha}

GitLab: return {web_repo}/-/commit/{commit_sha}

Bitbucket: @todo return {web_base}/{workspace}/{repo_slug}/commits/{commit_sha}

## `get_commits`

GitHub: [GET /repos/{owner}/{repo}/commits](https://docs.github.com/en/rest/commits/commits#list-commits)

GitLab: [GET /projects/:id/repository/commits](https://docs.gitlab.com/api/commits/#list-repository-commits)

Bitbucket: [GET /repositories/{workspace}/{repo_slug}/commits](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-commits/#api-repositories-workspace-repo-slug-commits-get)

## `get_commits_by_path`

GitHub: [GET /repos/{owner}/{repo}/commits](https://docs.github.com/en/rest/commits/commits#list-commits)

GitLab: [GET /projects/:id/repository/commits](https://docs.gitlab.com/api/commits/#list-repository-commits)

Bitbucket: [GET /repositories/{workspace}/{repo_slug}/filehistory/{commit}/{path}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-source/#api-repositories-workspace-repo-slug-filehistory-commit-path-get)

## `get_directory_contents`

GitHub: [GET /repos/{owner}/{repo}/contents/{path}](https://docs.github.com/en/rest/repos/contents#get-repository-content)

GitLab: [GET /projects/:id/repository/tree](https://docs.gitlab.com/api/repositories/#list-all-repository-trees-in-a-project)

Bitbucket: @todo Use [GET /repositories/{workspace}/{repo_slug}/src/{commit}/{path}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-source/#api-repositories-workspace-repo-slug-src-commit-path-get)

## `get_file_content`

GitHub: [GET /repos/{owner}/{repo}/contents/{path}](https://docs.github.com/en/rest/repos/contents#get-repository-content)

GitLab: [GET /projects/:id/repository/files/:file_path](https://docs.gitlab.com/api/repository_files/#retrieve-a-file-from-a-repository)

Bitbucket: [GET /repositories/{workspace}/{repo_slug}/src/{commit}/{path}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-source/#api-repositories-workspace-repo-slug-src-commit-path-get)

## `get_file_url`

GitHub: return {web_base}/{owner}/{repo}/blob/{sha}/{file_path}

GitLab: return {web_repo}/-/blob/{sha}/{file_path}

Bitbucket: @todo return {web_base}/{workspace}/{repo_slug}/src/{sha}/{file_path}

## `get_full_tree`

GitHub: [GET /repos/{owner}/{repo}/git/trees/{tree_sha}](https://docs.github.com/en/rest/git/trees#get-a-tree)

GitLab: [GET /projects/:id/repository/tree](https://docs.gitlab.com/api/repositories/#list-all-repository-trees-in-a-project)

Bitbucket: @todo Use [GET /repositories/{workspace}/{repo_slug}/src/{commit}/{path}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-source/#api-repositories-workspace-repo-slug-src-commit-path-get) (recursive listing, walking every page)

## `get_git_commit`

GitHub: [GET /repos/{owner}/{repo}/git/commits/{sha}](https://docs.github.com/en/rest/git/commits#get-a-commit-object)

GitLab: [GET /projects/:id/repository/commits/:sha](https://docs.gitlab.com/api/commits/#retrieve-a-commit)

Bitbucket: @todo Use [GET /repositories/{workspace}/{repo_slug}/commit/{commit}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-commits/#api-repositories-workspace-repo-slug-commit-commit-get)

## `get_git_ref`

GitHub: [GET /repos/{owner}/{repo}/git/ref/{ref}](https://docs.github.com/en/rest/git/refs#get-a-reference)

GitLab: *not implemented*

Bitbucket: *not implemented*

## `get_issue`

GitHub: [GET /repos/{owner}/{repo}/issues/{issue_number}](https://docs.github.com/en/rest/issues/issues#get-an-issue)

GitLab: [GET /projects/:id/issues/:issue_iid](https://docs.gitlab.com/api/issues/#retrieve-a-project-issue)

Bitbucket: *not supported* (issue tracker deprecated)

## `get_issue_comment_reactions`

GitHub: [GET /repos/{owner}/{repo}/issues/comments/{comment_id}/reactions](https://docs.github.com/en/rest/reactions/reactions#list-reactions-for-an-issue-comment)

GitLab: [GET /projects/:id/issues/:issue_iid/notes/:note_id/award_emoji](https://docs.gitlab.com/api/emoji_reactions/#list-all-emoji-reactions-for-a-comment)

Bitbucket: *not supported* (no emoji reactions)

## `get_issue_comments`

GitHub: [GET /repos/{owner}/{repo}/issues/{issue_number}/comments](https://docs.github.com/en/rest/issues/comments#list-issue-comments)

GitLab: [GET /projects/:id/issues/:issue_iid/notes](https://docs.gitlab.com/api/notes/#list-all-issue-notes)

Bitbucket: *not supported* (issue tracker deprecated)

## `get_issue_reactions`

GitHub: [GET /repos/{owner}/{repo}/issues/{issue_number}/reactions](https://docs.github.com/en/rest/reactions/reactions#list-reactions-for-an-issue)

GitLab: [GET /projects/:id/issues/:issue_iid/award_emoji](https://docs.gitlab.com/api/emoji_reactions/#list-all-emoji-reactions-for-a-resource)

Bitbucket: *not supported* (no emoji reactions)

## `get_pull_request`

GitHub: [GET /repos/{owner}/{repo}/pulls/{pull_number}](https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request)

GitLab: [GET /projects/:id/merge_requests/:merge_request_iid](https://docs.gitlab.com/api/merge_requests/#retrieve-a-merge-request)

Bitbucket: [GET /repositories/{workspace}/{repo_slug}/pullrequests/{pull_request_id}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-get)

## `get_pull_request_comment_reactions`

GitHub: [GET /repos/{owner}/{repo}/issues/comments/{comment_id}/reactions](https://docs.github.com/en/rest/reactions/reactions#list-reactions-for-an-issue-comment)

GitLab: [GET /projects/:id/merge_requests/:merge_request_iid/notes/:note_id/award_emoji](https://docs.gitlab.com/api/emoji_reactions/#list-all-emoji-reactions-for-a-comment)

Bitbucket: *not supported* (no emoji reactions)

## `get_pull_request_comments`

GitHub: [GET /repos/{owner}/{repo}/issues/{pull_number}/comments](https://docs.github.com/en/rest/issues/comments#list-issue-comments)

GitLab: [GET /projects/:id/merge_requests/:merge_request_iid/notes](https://docs.gitlab.com/api/notes/#list-all-merge-request-notes)

Bitbucket: [GET /repositories/{workspace}/{repo_slug}/pullrequests/{pull_request_id}/comments](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-comments-get)

## `get_pull_request_commits`

GitHub: [GET /repos/{owner}/{repo}/pulls/{pull_number}/commits](https://docs.github.com/en/rest/pulls/pulls#list-commits-on-a-pull-request)

GitLab: [GET /projects/:id/merge_requests/:merge_request_iid/commits](https://docs.gitlab.com/api/merge_requests/#retrieve-merge-request-commits)

Bitbucket: @todo Use [GET /repositories/{workspace}/{repo_slug}/pullrequests/{pull_request_id}/commits](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-commits-get)

## `get_pull_request_diff`

GitHub: [GET /repos/{owner}/{repo}/pulls/{pull_number}](https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request) with `Accept: application/vnd.github.v3.diff`

GitLab: [GET /projects/:id/merge_requests/:merge_request_iid/raw_diffs](https://docs.gitlab.com/api/merge_requests/#show-merge-request-raw-diffs)

Bitbucket: @todo Use [GET /repositories/{workspace}/{repo_slug}/pullrequests/{pull_request_id}/diff](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-diff-get)

## `get_pull_request_files`

GitHub: [GET /repos/{owner}/{repo}/pulls/{pull_number}/files](https://docs.github.com/en/rest/pulls/pulls#list-pull-requests-files)

GitLab: [GET /projects/:id/merge_requests/:merge_request_iid/diffs](https://docs.gitlab.com/api/merge_requests/#list-merge-request-diffs)

Bitbucket: @todo Use [GET /repositories/{workspace}/{repo_slug}/pullrequests/{pull_request_id}/diffstat](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-diffstat-get)

## `get_pull_request_reactions`

GitHub: [GET /repos/{owner}/{repo}/issues/{issue_number}/reactions](https://docs.github.com/en/rest/reactions/reactions#list-reactions-for-an-issue)

GitLab: [GET /projects/:id/merge_requests/:merge_request_iid/award_emoji](https://docs.gitlab.com/api/emoji_reactions/#list-all-emoji-reactions-for-a-resource)

Bitbucket: *not supported* (no emoji reactions)

## `get_pull_request_review_threads`

GitHub: [`pullRequest.reviewThreads`](https://docs.github.com/en/graphql/reference/pulls#object-pullrequest)

GitLab: [GET /projects/:id/merge_requests/:merge_request_iid/discussions](https://docs.gitlab.com/api/discussions/#list-all-merge-request-discussion-items)

Bitbucket: @todo Use [GET /repositories/{workspace}/{repo_slug}/pullrequests/{pull_request_id}/comments](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-comments-get) (inline comments)

## `get_pull_request_template`

GitHub: [GET /repos/{owner}/{repo}/contents/{path}](https://docs.github.com/en/rest/repos/contents#get-repository-content)

GitLab:
- [GET /projects/:id/repository/tree](https://docs.gitlab.com/api/repositories/#list-all-repository-trees-in-a-project)
- [GET /projects/:id/repository/files/:file_path](https://docs.gitlab.com/api/repository_files/#retrieve-a-file-from-a-repository)

Bitbucket: @todo Use:
- [GET /repositories/{workspace}/{repo_slug}/src/{commit}/{path}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-source/#api-repositories-workspace-repo-slug-src-commit-path-get) (lists the template directory)
- [GET /repositories/{workspace}/{repo_slug}/src/{commit}/{path}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-source/#api-repositories-workspace-repo-slug-src-commit-path-get) (fetches each template)

## `get_pull_request_url`

GitHub: return {web_base}/{owner}/{repo}/pull/{pull_number}

GitLab: return {web_repo}/-/merge_requests/{pull_request}

Bitbucket: @todo return {web_base}/{workspace}/{repo_slug}/pull-requests/{pull_request_id}

## `get_pull_requests`

GitHub: [GET /repos/{owner}/{repo}/pulls](https://docs.github.com/en/rest/pulls/pulls#list-pull-requests)

GitLab: [GET /projects/:id/merge_requests](https://docs.gitlab.com/api/merge_requests/#list-project-merge-requests)

Bitbucket: [GET /repositories/{workspace}/{repo_slug}/pullrequests](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-get)

## `get_readme`

GitHub: [GET /repos/{owner}/{repo}/readme](https://docs.github.com/en/rest/repos/contents#get-a-repository-readme)

GitLab:
- [GET /projects/:id/repository/tree](https://docs.gitlab.com/api/repositories/#list-all-repository-trees-in-a-project)
- [GET /projects/:id/repository/files/:file_path](https://docs.gitlab.com/api/repository_files/#retrieve-a-file-from-a-repository)

Bitbucket: @todo Use:
- [GET /repositories/{workspace}/{repo_slug}/src/{commit}/{path}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-source/#api-repositories-workspace-repo-slug-src-commit-path-get) (lists the repo root)
- [GET /repositories/{workspace}/{repo_slug}/src/{commit}/{path}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-source/#api-repositories-workspace-repo-slug-src-commit-path-get) (fetches the README)

## `get_repository`

GitHub: [GET /repos/{owner}/{repo}](https://docs.github.com/en/rest/repos/repos#get-a-repository)

GitLab: [GET /projects/:id](https://docs.gitlab.com/api/projects/#retrieve-a-project)

Bitbucket: [GET /repositories/{workspace}/{repo_slug}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-repositories/#api-repositories-workspace-repo-slug-get)

## `get_repository_assignees`

GitHub: [GET /repos/{owner}/{repo}/assignees](https://docs.github.com/en/rest/issues/assignees#list-assignees)

GitLab: [GET /projects/:id/users](https://docs.gitlab.com/api/projects/#list-all-members-of-a-project)

Bitbucket: @todo Use [GET /workspaces/{workspace}/permissions/repositories/{repo_slug}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-workspaces/#api-workspaces-workspace-permissions-repositories-repo-slug-get) (admin only)

## `get_repository_labels`

GitHub: [GET /repos/{owner}/{repo}/labels](https://docs.github.com/en/rest/issues/labels#list-labels-for-a-repository)

GitLab: [GET /projects/:id/labels](https://docs.gitlab.com/api/labels/#list-all-project-labels)

Bitbucket: *not supported* (issue tracker deprecated)

## `get_repository_topics`

GitHub: [GET /repos/{owner}/{repo}/topics](https://docs.github.com/en/rest/repos/repos#get-all-repository-topics)

GitLab: [GET /projects/:id](https://docs.gitlab.com/api/projects/#retrieve-a-project)

Bitbucket: *not supported* (no topics)

## `get_repository_user_permission`

GitHub: [GET /repos/{owner}/{repo}/collaborators/{username}/permission](https://docs.github.com/en/rest/collaborators/collaborators#get-repository-permissions-for-a-user)

GitLab: *not implemented*

Bitbucket: *not implemented*

## `get_review_comments`

GitHub: [GET /repos/{owner}/{repo}/pulls/{pull_number}/reviews/{review_id}/comments](https://docs.github.com/en/rest/pulls/reviews#list-comments-for-a-pull-request-review)

GitLab: *not implemented*

Bitbucket: *not implemented*

## `get_thread_id_from_review_comment_unique_id`

GitHub: [`pullRequest.reviewThreads`](https://docs.github.com/en/graphql/reference/pulls#object-pullrequest)

GitLab: local parsing

Bitbucket: @todo Find out

## `get_tree`

GitHub: [GET /repos/{owner}/{repo}/git/trees/{tree_sha}](https://docs.github.com/en/rest/git/trees#get-a-tree)

GitLab: [GET /projects/:id/repository/tree](https://docs.gitlab.com/api/repositories/#list-all-repository-trees-in-a-project)

Bitbucket: @todo Use [GET /repositories/{workspace}/{repo_slug}/src/{commit}/{path}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-source/#api-repositories-workspace-repo-slug-src-commit-path-get)

## `list_check_runs_for_ref`

GitHub: [GET /repos/{owner}/{repo}/commits/{ref}/check-runs](https://docs.github.com/en/rest/checks/runs#list-check-runs-for-a-git-reference)

GitLab: *not implemented*

Bitbucket: *not implemented*

## `list_check_runs_in_check_suite`

GitHub: [GET /repos/{owner}/{repo}/check-suites/{check_suite_id}/check-runs](https://docs.github.com/en/rest/checks/runs#list-check-runs-in-a-check-suite)

GitLab: *not implemented*

Bitbucket: *not implemented*

## `list_pull_request_reviews`

GitHub: [GET /repos/{owner}/{repo}/pulls/{pull_number}/reviews](https://docs.github.com/en/rest/pulls/reviews#list-reviews-for-a-pull-request)

GitLab: *not implemented*

Bitbucket: *not implemented*

## `list_repositories`

GitHub: [GET /installation/repositories](https://docs.github.com/en/rest/apps/installations#list-repositories-accessible-to-the-app-installation)

GitLab: *not implemented*

Bitbucket: *not implemented*

## `list_repository_user_permissions`

GitHub: [GET /repos/{owner}/{repo}/collaborators](https://docs.github.com/en/rest/collaborators/collaborators#list-repository-collaborators)

GitLab: *not implemented*

Bitbucket: *not implemented*

## `list_workflow_jobs`

GitHub: [GET /repos/{owner}/{repo}/actions/runs/{run_id}/jobs](https://docs.github.com/en/rest/actions/workflow-jobs#list-jobs-for-a-workflow-run)

GitLab: *not implemented*

Bitbucket: *not implemented*

## `list_workflow_runs`

GitHub: [GET /repos/{owner}/{repo}/actions/runs](https://docs.github.com/en/rest/actions/workflow-runs#list-workflow-runs-for-a-repository)

GitLab: *not implemented*

Bitbucket: *not implemented*

## `minimize_comment`

GitHub: [`minimizeComment`](https://docs.github.com/en/graphql/reference/issues#mutation-minimizecomment)

GitLab: *not implemented*

Bitbucket: *not implemented*

## `request_review`

GitHub: [POST /repos/{owner}/{repo}/pulls/{pull_number}/requested_reviewers](https://docs.github.com/en/rest/pulls/review-requests#request-reviewers-for-a-pull-request)

GitLab: *not implemented*

Bitbucket: *not implemented*

## `resolve_review_thread`

GitHub: [`resolveReviewThread`](https://docs.github.com/en/graphql/reference/pulls#mutation-resolvereviewthread)

GitLab: [PUT /projects/:id/merge_requests/:merge_request_iid/discussions/:discussion_id](https://docs.gitlab.com/api/discussions/#resolve-a-merge-request-thread)

Bitbucket: @todo Use [POST /repositories/{workspace}/{repo_slug}/pullrequests/{pull_request_id}/comments/{comment_id}/resolve](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-comments-comment-id-resolve-post)

## `update_and_collapse_pull_request_comment`

GitHub: [`updatePullRequestReviewComment`](https://docs.github.com/en/graphql/reference/pulls#mutation-updatepullrequestreviewcomment) and either [`resolveReviewThread`](https://docs.github.com/en/graphql/reference/pulls#mutation-resolvereviewthread) or [`minimizeComment`](https://docs.github.com/en/graphql/reference/issues#mutation-minimizecomment)

GitLab:
- [PUT /projects/:id/merge_requests/:merge_request_iid/discussions/:discussion_id/notes/:note_id](https://docs.gitlab.com/api/discussions/#update-a-merge-request-thread-note)
- [PUT /projects/:id/merge_requests/:merge_request_iid/discussions/:discussion_id](https://docs.gitlab.com/api/discussions/#resolve-a-merge-request-thread)

Bitbucket: @todo Use:
- [PUT /repositories/{workspace}/{repo_slug}/pullrequests/{pull_request_id}/comments/{comment_id}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-comments-comment-id-put)
- [POST /repositories/{workspace}/{repo_slug}/pullrequests/{pull_request_id}/comments/{comment_id}/resolve](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-comments-comment-id-resolve-post)

## `update_branch`

GitHub: [PATCH /repos/{owner}/{repo}/git/refs/heads/{branch}](https://docs.github.com/en/rest/git/refs#update-a-reference)

GitLab: *not implemented*

Bitbucket: *not implemented*

## `update_check_run`

GitHub: [PATCH /repos/{owner}/{repo}/check-runs/{check_run_id}](https://docs.github.com/en/rest/checks/runs#update-a-check-run)

GitLab: [POST /projects/:id/statuses/:sha](https://docs.gitlab.com/api/commits/#set-commit-pipeline-status)

Bitbucket: @todo Use [PUT /repositories/{workspace}/{repo_slug}/commit/{commit}/statuses/build/{key}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-commit-statuses/#api-repositories-workspace-repo-slug-commit-commit-statuses-build-key-put)

## `update_issue`

GitHub: [PATCH /repos/{owner}/{repo}/issues/{issue_number}](https://docs.github.com/en/rest/issues/issues#update-an-issue)

GitLab: [PUT /projects/:id/issues/:issue_iid](https://docs.gitlab.com/api/issues/#update-an-issue)

Bitbucket: *not supported* (issue tracker deprecated)

## `update_pull_request`

GitHub: [PATCH /repos/{owner}/{repo}/pulls/{pull_number}](https://docs.github.com/en/rest/pulls/pulls#update-a-pull-request)

GitLab: [PUT /projects/:id/merge_requests/:merge_request_iid](https://docs.gitlab.com/api/merge_requests/#update-a-merge-request)

Bitbucket:
- [PUT /repositories/{workspace}/{repo_slug}/pullrequests/{pull_request_id}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-put)
- [POST /repositories/{workspace}/{repo_slug}/pullrequests/{pull_request_id}/decline](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-decline-post)

## `update_review_comment`

GitHub: [PATCH /repos/{owner}/{repo}/pulls/comments/{comment_id}](https://docs.github.com/en/rest/pulls/comments#update-a-review-comment-for-a-pull-request)

GitLab: [PUT /projects/:id/merge_requests/:merge_request_iid/discussions/:discussion_id/notes/:note_id](https://docs.gitlab.com/api/discussions/#update-a-merge-request-thread-note)

Bitbucket: @todo Use [PUT /repositories/{workspace}/{repo_slug}/pullrequests/{pull_request_id}/comments/{comment_id}](https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-comments-comment-id-put)

