import msgspec

from scm.providers.github.types import GitHubPullRequestEvent


def test_github_pull_request_event_accepts_stacked_action() -> None:
    event = msgspec.json.decode(
        b"""{
            "action": "stacked",
            "number": 1,
            "pull_request": {
                "body": null,
                "head": {"ref": "feature", "repo": null, "sha": "abc123"},
                "base": {"ref": "main", "repo": {"private": false}, "sha": "def456"},
                "merge_commit_sha": null,
                "title": "Stacked pull request",
                "user": {"id": 1, "login": "testuser"}
            }
        }""",
        type=GitHubPullRequestEvent,
    )

    assert event.action == "stacked"
