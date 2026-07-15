import base64
import dataclasses
import json
import os
import pathlib
import re
import time
from collections.abc import Callable, Sequence
from concurrent.futures import Future
from datetime import datetime
from typing import Any, Literal

import jwt
import pytest
import requests
import requests.auth

from scm.errors import PathIsNotDirectory, SCMError
from scm.manager import SourceCodeManager
from scm.providers.bitbucket.provider import BitbucketProvider
from scm.providers.github.provider import GitHubProvider
from scm.providers.gitlab.provider import API_VERSION, GitLabProvider
from scm.rpc.client import NoOpRateLimiter
from scm.types import (
    CollapsePullRequestCommentProtocol,
    CompareCommitsProtocol,
    CreateBranchProtocol,
    CreateCheckRunProtocol,
    CreateCommitProtocol,
    CreateIssueCommentProtocol,
    CreateIssueCommentReactionProtocol,
    CreateIssueReactionProtocol,
    CreatePullRequestCommentProtocol,
    CreatePullRequestCommentReactionProtocol,
    CreatePullRequestDraftProtocol,
    CreatePullRequestProtocol,
    CreatePullRequestReactionProtocol,
    CreateReviewCommentFileProtocol,
    CreateReviewCommentLineProtocol,
    CreateReviewCommentMultilineProtocol,
    CreateReviewCommentReplyProtocol,
    CreateReviewProtocol,
    CredentialsSet,
    DeleteBranchProtocol,
    DeleteIssueCommentProtocol,
    DeleteIssueCommentReactionProtocol,
    DeleteIssueReactionProtocol,
    DeletePullRequestCommentProtocol,
    DeletePullRequestCommentReactionProtocol,
    DeletePullRequestReactionProtocol,
    GetAppInstallationProtocol,
    GetArchiveLinkProtocol,
    GetAuthenticatedActorProtocol,
    GetBranchProtocol,
    GetCheckRunProtocol,
    GetCommitChangesProtocol,
    GetCommitProtocol,
    GetCommitsByPathProtocol,
    GetCommitsProtocol,
    GetCommitUrlProtocol,
    GetDirectoryContentsProtocol,
    GetFileContentProtocol,
    GetFileUrlProtocol,
    GetFullTreeProtocol,
    GetGitCommitProtocol,
    GetIssueCommentReactionsProtocol,
    GetIssueCommentsProtocol,
    GetIssueReactionsProtocol,
    GetPullRequestCommentReactionsProtocol,
    GetPullRequestCommentsProtocol,
    GetPullRequestCommitsProtocol,
    GetPullRequestDiffProtocol,
    GetPullRequestFilesProtocol,
    GetPullRequestProtocol,
    GetPullRequestReactionsProtocol,
    GetPullRequestReviewThreadsProtocol,
    GetPullRequestsProtocol,
    GetPullRequestTemplateProtocol,
    GetPullRequestUrlProtocol,
    GetReadmeProtocol,
    GetRepositoryAssigneesProtocol,
    GetRepositoryProtocol,
    GetTreeProtocol,
    PaginationParams,
    Provider,
    Repository,
    RepositoryId,
    ResolveReviewThreadProtocol,
    UpdateAndCollapsePullRequestCommentProtocol,
    UpdateBranchProtocol,
    UpdateCheckRunProtocol,
    UpdatePullRequestProtocol,
    UpdateReviewCommentProtocol,
    WriteCommitAction,
)

# GitHub client
# -------------

GITHUB_API_BASE = "https://api.github.com"


class InstallationTokenManager:
    """Manages GitHub App installation access tokens with automatic refresh.

    On replay there is no private key, so ``_make_jwt`` returns a placeholder --
    the token-exchange POST it authenticates is served from the cassette anyway.
    """

    def __init__(self, app_id: str, private_key: str, installation_id: str, base_url: str) -> None:
        self.app_id = app_id
        self.private_key = private_key
        self.installation_id = installation_id
        self.base_url = base_url
        self._token: str | None = None
        self._expires_at: float = 0

    def _make_jwt(self) -> str:
        if not self.private_key:
            return "replay-placeholder-jwt"
        now = int(time.time())
        payload = {"iat": now - 60, "exp": now + (10 * 60), "iss": self.app_id}
        return jwt.encode(payload, self.private_key, algorithm="RS256")

    def _refresh(self) -> None:
        token = self._make_jwt()
        response = requests.post(
            f"{self.base_url}/app/installations/{self.installation_id}/access_tokens",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        )
        response.raise_for_status()
        self._token = response.json()["token"]
        self._expires_at = time.time() + 3300

    @property
    def application_token(self) -> str:
        return self._make_jwt()

    @property
    def installation_token(self) -> str:
        if self._token is None or time.time() >= self._expires_at:
            self._refresh()
        assert self._token is not None
        return self._token


class GitHubApiClient:
    def __init__(self, token_manager: InstallationTokenManager, base_url: str) -> None:
        self.token_manager = token_manager
        self.base_url = base_url

    def request(
        self,
        method: str,
        path: str,
        headers: dict[str, str] | None = None,
        data: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
        allow_redirects: bool | None = None,
        stream: bool = True,
        raw_response: bool = True,
        credentials_set: CredentialsSet = "installation",
        timeout: float | tuple[float, float] | None = None,
    ) -> requests.Response:
        url = f"{self.base_url}{path}"
        token = (
            self.token_manager.installation_token
            if credentials_set == "installation"
            else self.token_manager.application_token
        )
        req_headers = {"Authorization": f"Bearer {token}"}
        if headers:
            req_headers.update(headers)

        kwargs: dict[str, Any] = {"headers": req_headers}
        if data is not None:
            kwargs["json"] = data
        if params is not None:
            kwargs["params"] = params
        if allow_redirects is not None:
            kwargs["allow_redirects"] = allow_redirects
        kwargs["stream"] = stream

        return requests.request(method, url, **kwargs)


def _github_provider(creds: dict[str, str], organization_id: int, external_id: str) -> Provider:
    base_url = resolve("GITHUB_BASE_URL", creds, GITHUB_API_BASE)
    assert base_url is not None
    private_key_path = resolve("GITHUB_PRIVATE_KEY_PATH", creds)
    private_key = pathlib.Path(private_key_path).read_text() if private_key_path else ""

    token_manager = InstallationTokenManager(
        app_id=resolve("GITHUB_APP_ID", creds, "1") or "1",
        private_key=private_key,
        installation_id=resolve("GITHUB_INSTALLATION_ID", creds, "1") or "1",
        base_url=base_url,
    )
    web_base_url = "https://github.com"
    repository: Repository = {
        "id": 1,
        "external_id": external_id,
        "integration_id": 1,
        "is_active": True,
        "name": external_id,
        "organization_id": organization_id,
        "provider_name": "github",
        "web_base_url": web_base_url,
    }
    return GitHubProvider(
        client=GitHubApiClient(token_manager, base_url=base_url),
        organization_id=organization_id,
        repository=repository,
        rate_limiter=NoOpRateLimiter(),
        web_base_url=web_base_url,
    )


# GitLab client
# -------------


class GitLabApiClient:
    def __init__(self, base_url: str, access_token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.access_token = access_token

    def request(
        self,
        method: str,
        path: str,
        headers: dict[str, str] | None = None,
        data: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
        allow_redirects: bool | None = None,
        stream: bool = True,
        raw_response: bool = True,
        credentials_set: CredentialsSet = "installation",
        timeout: float | tuple[float, float] | None = None,
    ) -> requests.Response:
        url = f"{self.base_url}{API_VERSION}{path}"
        req_headers = {"Authorization": f"Bearer {self.access_token}"}
        if headers:
            req_headers.update(headers)

        kwargs: dict[str, Any] = {"headers": req_headers}
        if data is not None:
            kwargs["json"] = data
        if params is not None:
            kwargs["params"] = params
        if allow_redirects is not None:
            kwargs["allow_redirects"] = allow_redirects
        if stream is not None:
            kwargs["stream"] = stream

        return requests.request(method, url, **kwargs)


def _gitlab_provider(creds: dict[str, str], organization_id: int, external_id: str) -> Provider:
    base_url = resolve("GITLAB_BASE_URL", creds, "https://gitlab.com")
    assert base_url is not None
    access_token = resolve("GITLAB_ACCESS_TOKEN", creds, "") or ""
    repository: Repository = {
        "id": 1,
        # external_id is "{netloc}:{project_id}" (drives the API and web_base_url); the
        # client fixture already includes the netloc.
        "external_id": external_id,
        "integration_id": 1,
        "is_active": True,
        # name is the human full path, used to build web URLs (the API uses project_id).
        "name": "jacquev6-sentry/test-sentry-integration-dev-jacquev6",
        "organization_id": organization_id,
        "provider_name": "gitlab",
        "web_base_url": None,
    }
    return GitLabProvider(
        client=GitLabApiClient(base_url, access_token),
        organization_id=organization_id,
        repository=repository,
    )


# Bitbucket client
# ----------------


@dataclasses.dataclass
class BitbucketCredentials:
    email: str
    api_token: str


class BitbucketApiClient:
    def __init__(self, credentials: BitbucketCredentials) -> None:
        self.credentials = credentials

    def request(
        self,
        method: str,
        path: str,
        headers: dict[str, str] | None = None,
        data: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
        allow_redirects: bool | None = None,
        stream: bool = True,
        raw_response: bool = True,
        credentials_set: CredentialsSet = "installation",
        timeout: float | tuple[float, float] | None = None,
    ) -> requests.Response:
        url = f"https://api.bitbucket.org/2.0{path}"

        kwargs: dict[str, Any] = {}
        if headers is not None:
            kwargs["headers"] = headers
        if data is not None:
            # Most Bitbucket writes are JSON, but the create-commit endpoint (POST /src)
            # is form-encoded; the provider signals that via the Content-Type header.
            content_type = next((v for k, v in (headers or {}).items() if k.lower() == "content-type"), "")
            if "form-urlencoded" in content_type:
                kwargs["data"] = data
            else:
                kwargs["json"] = data
        if params is not None:
            kwargs["params"] = params
        if allow_redirects is not None:
            kwargs["allow_redirects"] = allow_redirects
        if stream is not None:
            kwargs["stream"] = stream

        return requests.request(
            method,
            url,
            auth=requests.auth.HTTPBasicAuth(self.credentials.email, self.credentials.api_token),
            **kwargs,
        )


def _bitbucket_provider(creds: dict[str, str], organization_id: int, external_id: str) -> Provider:
    credentials = BitbucketCredentials(
        email=resolve("BITBUCKET_EMAIL", creds, "") or "",
        api_token=resolve("BITBUCKET_API_TOKEN", creds, "") or "",
    )
    repository: Repository = {
        "id": 1,
        "external_id": external_id,
        "integration_id": 1,
        "is_active": True,
        "name": external_id,
        "organization_id": organization_id,
        "provider_name": "bitbucket",
        "web_base_url": None,
    }
    return BitbucketProvider(
        client=BitbucketApiClient(credentials),
        organization_id=organization_id,
        repository=repository,
    )


# Client creation
# ---------------


def _repo_root() -> pathlib.Path:
    """Nearest ancestor holding pyproject.toml -- where .credentials lives."""
    for parent in pathlib.Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").exists():
            return parent
    return pathlib.Path(__file__).resolve().parent


def load_credentials() -> dict[str, str]:
    """Load KEY=VALUE pairs from .credentials, skipping blanks and comments.

    Returns an empty dict when the file is absent -- which is the normal case on
    replay, where credentials are not needed.
    """
    creds: dict[str, str] = {}
    path = _repo_root() / ".credentials"
    if not path.exists():
        return creds
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.strip()
        if value:
            creds[key.strip()] = value
    return creds


def resolve(key: str, creds: dict[str, str], default: str | None = None) -> str | None:
    """Env var wins, then .credentials file, then default."""
    return os.environ.get(key) or creds.get(key) or default


_PROVIDER_BUILDERS: dict[str, Callable[[dict[str, str], int, str], Provider]] = {
    "github": _github_provider,
    "gitlab": _gitlab_provider,
    "bitbucket": _bitbucket_provider,
}


def make_client(service: str, organization_id: int, repository_id: RepositoryId) -> SourceCodeManager:
    """Build a ``SourceCodeManager`` via ``make_client`` whose provider talks
    directly to the real service through ``requests`` (so vcrpy can record it).

    Goes through ``make_client`` -- rather than instantiating the facade directly
    -- to exercise the same ``initialize_provider`` validation path as production.
    """
    creds = load_credentials()
    assert isinstance(repository_id, (list, tuple))
    _, external_id = repository_id
    provider = _PROVIDER_BUILDERS[service](creds, organization_id, external_id)
    return SourceCodeManager.make_client(
        organization_id,
        repository_id,
        fetch_repository=lambda oid, rid: provider.repository,
        fetch_provider=lambda oid, repo: provider,
        record_count=lambda name, value, tags: None,
    )


# VCR setup
# ---------

# These tests run in-process against recorded HTTP interactions (vcrpy cassettes
# under ./cassettes). On the first run for a test the cassette is recorded live,
# which requires credentials in .credentials and resources (repos, PRs, etc.) on
# GitHub/GitLab/Bitbucket in the expected state; every run after that replays from
# disk and needs neither.

pytestmark = pytest.mark.vcr


# The GitHub App installation id appears in the token-exchange URL, which is part
# of what vcrpy matches on. It comes from .credentials, so without normalization
# replay would only match on the machine that recorded. Rewriting it to a fixed
# placeholder -- applied on both record and replay -- keeps replay credential-free
# (and keeps the real id out of committed cassettes).
_GH_INSTALLATION_PATH = re.compile(r"/app/installations/[^/]+/access_tokens")


def _normalize_request(request: Any) -> Any:
    request.uri = _GH_INSTALLATION_PATH.sub("/app/installations/_/access_tokens", request.uri)
    return request


def _scrub_response(response: dict[str, Any]) -> dict[str, Any]:
    """
    Redact secrets from recorded response bodies before they hit disk.

    The GitHub installation-token exchange returns a live ``token`` -- replace it
    so cassettes are safe to commit. Matching never looks at bodies, so the
    placeholder is harmless on replay.
    """
    body = response.get("body", {}).get("string")
    if not body:
        return response
    try:
        payload = json.loads(body)
    except (ValueError, TypeError):
        return response
    if isinstance(payload, dict) and "token" in payload:
        payload["token"] = "SCRUBBED"
        # vcr response bodies must be bytes; json.dumps yields str.
        scrubbed = json.dumps(payload)
        response["body"]["string"] = scrubbed.encode() if isinstance(body, bytes) else scrubbed
    return response


@pytest.fixture(scope="module")
def vcr_config() -> dict[str, Any]:
    return {
        "record_mode": "once",
        "match_on": ["method", "scheme", "host", "port", "path", "query"],
        "filter_headers": [
            ("authorization", "SCRUBBED"),
            ("x-credentials-set", None),
            ("cookie", None),
            ("set-cookie", None),
        ],
        "decode_compressed_response": True,
        "before_record_request": _normalize_request,
        "before_record_response": _scrub_response,
    }


# Actual tests
# ------------


@pytest.fixture
def now() -> datetime:
    """A fixed 'current time' so values the tests generate and then assert on
    (branch names, check-run names, review bodies) round-trip identically on
    replay. Using a constant -- rather than freezing the clock -- keeps real time
    for GitHub App JWT signing on the recording run."""
    return datetime(2026, 7, 8, 12, 0, 0)


class _InlineExecutor:
    """Drop-in for ThreadPoolExecutor that runs submitted work synchronously.

    Several provider actions (e.g. GitLab/Bitbucket create_review) fan out their
    HTTP calls across a thread pool. vcrpy's cassette is not thread-safe:
    concurrent requests race both when recording (interleaved appends) and when
    replaying (matching and marking interactions played), and the order they hit
    the cassette is nondeterministic. Running inline keeps every request on one
    thread in submission order, which is what VCR needs -- without changing the
    provider code itself.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def __enter__(self) -> "_InlineExecutor":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def submit(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Future:
        # Mirror ThreadPoolExecutor: capture failures on the future, re-raise at .result().
        future: Future = Future()
        try:
            future.set_result(fn(*args, **kwargs))
        except Exception as exc:
            future.set_exception(exc)
        return future

    def map(self, fn: Callable[..., Any], *iterables: Any, **kwargs: Any) -> "map[Any]":
        # Results in input order, same as Executor.map -- just computed inline.
        return map(fn, *iterables)

    def shutdown(self, *args: Any, **kwargs: Any) -> None:
        pass


@pytest.fixture(autouse=True)
def _serialize_provider_fanout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force provider thread-pool fan-out to run inline so vcrpy stays happy."""
    for module in ("scm.providers.gitlab.provider", "scm.providers.bitbucket.provider"):
        monkeypatch.setattr(f"{module}.ThreadPoolExecutor", _InlineExecutor)


@pytest.fixture(
    params=(
        params := [
            # https://github.com/jacquev6/test-Sentry-Integration-Dev-jacquev6
            ("github", "jacquev6/test-Sentry-Integration-Dev-jacquev6"),
            # https://gitlab.com/jacquev6-sentry/test-sentry-integration-dev-jacquev6
            ("gitlab", "gitlab.com:79787061"),
            # https://bitbucket.org/jacquev6-sentry/test-sentry-integration-dev-jacquev6
            ("bitbucket", "jacquev6-sentry/test-sentry-integration-dev-jacquev6"),
        ]
    ),
    ids=[p[0] for p in params],
)
def client(request: pytest.FixtureRequest) -> SourceCodeManager:
    service = request.param[0]
    return make_client(service, organization_id=1, repository_id=request.param)


type Service = Literal["github", "gitlab", "bitbucket"]


@pytest.fixture
def service(request: pytest.FixtureRequest) -> Service:
    return request.node.callspec.params["client"][0].lower()


type Switch[T] = Callable[[T, T, T], T]


@pytest.fixture
def switch(service: Service) -> Switch:
    def f[T](github: T, gitlab: T, bitbucket: T) -> T:
        match service:
            case "github":
                return github
            case "gitlab":
                return gitlab
            case "bitbucket":
                return bitbucket

    return f


def test_get_repository(switch: Switch, client: SourceCodeManager) -> None:
    assert isinstance(client, GetRepositoryProtocol)
    assert client.get_repository()["data"] == {
        "full_name": switch(
            "jacquev6/test-Sentry-Integration-Dev-jacquev6",
            "jacquev6-sentry/test-sentry-integration-dev-jacquev6",
            "jacquev6-sentry/test-sentry-integration-dev-jacquev6",
        ),
        "default_branch": "main",
        "description": "Test repo for my developments in Sentry's App",
        "clone_url": switch(
            "https://github.com/jacquev6/test-Sentry-Integration-Dev-jacquev6.git",
            "https://gitlab.com/jacquev6-sentry/test-sentry-integration-dev-jacquev6.git",
            "https://jacquev6@bitbucket.org/jacquev6-sentry/test-sentry-integration-dev-jacquev6.git",
        ),
        "private": False,
        "size": switch(1, 7, 5),
        "topics": [],
    }


def test_url_builders(switch: Switch, client: SourceCodeManager) -> None:
    # Pure URL builders -- no HTTP, so no cassette is recorded.
    assert isinstance(client, GetCommitUrlProtocol)
    assert client.get_commit_url("7497e018d01503b6abc3053b7896266115e631f6") == switch(
        "https://github.com/jacquev6/test-Sentry-Integration-Dev-jacquev6/commit/7497e018d01503b6abc3053b7896266115e631f6",
        "https://gitlab.com/jacquev6-sentry/test-sentry-integration-dev-jacquev6/-/commit/7497e018d01503b6abc3053b7896266115e631f6",
        "https://bitbucket.org/jacquev6-sentry/test-sentry-integration-dev-jacquev6/commits/7497e018d01503b6abc3053b7896266115e631f6",
    )

    assert isinstance(client, GetPullRequestUrlProtocol)
    assert client.get_pull_request_url("2") == switch(
        "https://github.com/jacquev6/test-Sentry-Integration-Dev-jacquev6/pull/2",
        "https://gitlab.com/jacquev6-sentry/test-sentry-integration-dev-jacquev6/-/merge_requests/2",
        "https://bitbucket.org/jacquev6-sentry/test-sentry-integration-dev-jacquev6/pull-requests/2",
    )

    assert isinstance(client, GetFileUrlProtocol)
    assert client.get_file_url(
        "BLAH.md", "7497e018d01503b6abc3053b7896266115e631f6", start_line=5, end_line=10
    ) == switch(
        "https://github.com/jacquev6/test-Sentry-Integration-Dev-jacquev6/blob/7497e018d01503b6abc3053b7896266115e631f6/BLAH.md#L5-L10",
        "https://gitlab.com/jacquev6-sentry/test-sentry-integration-dev-jacquev6/-/blob/7497e018d01503b6abc3053b7896266115e631f6/BLAH.md#L5-L10",
        "https://bitbucket.org/jacquev6-sentry/test-sentry-integration-dev-jacquev6/src/7497e018d01503b6abc3053b7896266115e631f6/BLAH.md#lines-5:10",
    )


def test_get_repository_assignees(switch: Switch, client: SourceCodeManager) -> None:
    assert isinstance(client, GetRepositoryAssigneesProtocol)
    assert client.get_repository_assignees()["data"] == [
        {"id": switch("327146", "150871", "{b116ba0e-54b0-48c1-8118-77624b8c8d33}"), "username": "jacquev6"}
    ]


def test_get_app_installation(client: SourceCodeManager) -> None:
    assert isinstance(client, GetAppInstallationProtocol)
    # The authenticated identity owns the test repo, so it has full access everywhere.
    assert client.get_app_installation()["data"] == {
        "has_read_access": True,
        "has_write_access": True,
        "has_check_run_write_access": True,
    }


def test_get_authenticated_actor(switch: Switch, client: SourceCodeManager) -> None:
    assert isinstance(client, GetAuthenticatedActorProtocol)
    actor = client.get_authenticated_actor()["data"]
    assert actor["username"] == switch("sentry-integration-dev-jacquev6[bot]", "jacquev6", "jacquev6")
    assert actor["id"] == switch("261902604", "150871", "{b116ba0e-54b0-48c1-8118-77624b8c8d33}")


def test_get_archive_link(service: str, switch: Switch, client: SourceCodeManager) -> None:
    assert isinstance(client, GetArchiveLinkProtocol) == (service == "github")

    if isinstance(client, GetArchiveLinkProtocol):
        link = client.get_archive_link("main")["data"]
        assert link["url"] == switch(
            "https://codeload.github.com/jacquev6/test-Sentry-Integration-Dev-jacquev6/legacy.tar.gz/refs/heads/main",
            "",
            "",
        )
        # assert link["headers"]["Authorization"].startswith(switch("token ", "Bearer ", None))

        assert client.get_archive_link("main", "zip")["data"]["url"] == switch(
            "https://codeload.github.com/jacquev6/test-Sentry-Integration-Dev-jacquev6/legacy.zip/refs/heads/main",
            "",
            "",
        )


def test_pull_requests(switch: Switch, now: datetime, client: SourceCodeManager) -> None:
    assert isinstance(client, GetPullRequestsProtocol)

    open_pull_requests = client.get_pull_requests()["data"]
    assert open_pull_requests == [
        {
            "id": switch("2", "1", "1"),
            "internal_id": switch("3329785233", "459277081", "1"),
            "title": "Add blah",
            "author": {
                "id": switch("327146", "150871", "{b116ba0e-54b0-48c1-8118-77624b8c8d33}"),
                "username": "jacquev6",
            },
            "body": "Blah blah blah.",
            "state": "open",
            "merged": False,
            "html_url": switch(
                "https://github.com/jacquev6/test-Sentry-Integration-Dev-jacquev6/pull/2",
                "https://gitlab.com/jacquev6-sentry/test-sentry-integration-dev-jacquev6/-/merge_requests/1",
                "https://bitbucket.org/jacquev6-sentry/test-sentry-integration-dev-jacquev6/pull-requests/1",
            ),
            "head": {
                "sha": switch(
                    "7497e018d01503b6abc3053b7896266115e631f6",
                    "7497e018d01503b6abc3053b7896266115e631f6",
                    "7497e018d015",
                ),
                "ref": "topics/blah",
            },
            "base": {
                "sha": switch("0941ee0a9eac9914cfddf5adec7a9558a2f1c447", None, "0941ee0a9eac"),
                "ref": "main",
            },
        }
    ]
    assert isinstance(client, GetPullRequestProtocol)
    assert client.get_pull_request(switch("2", "1", "1"))["data"] == {
        "id": switch("2", "1", "1"),
        "internal_id": switch("3329785233", "459277081", "1"),
        "title": "Add blah",
        "author": {
            "id": switch("327146", "150871", "{b116ba0e-54b0-48c1-8118-77624b8c8d33}"),
            "username": "jacquev6",
        },
        "body": "Blah blah blah.",
        "state": "open",
        "merged": False,
        "html_url": switch(
            "https://github.com/jacquev6/test-Sentry-Integration-Dev-jacquev6/pull/2",
            "https://gitlab.com/jacquev6-sentry/test-sentry-integration-dev-jacquev6/-/merge_requests/1",
            "https://bitbucket.org/jacquev6-sentry/test-sentry-integration-dev-jacquev6/pull-requests/1",
        ),
        "head": {
            "sha": switch(
                "7497e018d01503b6abc3053b7896266115e631f6", "7497e018d01503b6abc3053b7896266115e631f6", "7497e018d015"
            ),
            "ref": "topics/blah",
        },
        "base": {
            "sha": switch(
                "0941ee0a9eac9914cfddf5adec7a9558a2f1c447", "0941ee0a9eac9914cfddf5adec7a9558a2f1c447", "0941ee0a9eac"
            ),
            "ref": "main",
        },
    }

    assert isinstance(client, UpdatePullRequestProtocol)
    edited = client.update_pull_request(switch("2", "1", "1"), title="Foo")["data"]
    assert edited["title"] == "Foo"
    assert edited["body"] == "Blah blah blah."
    edited = client.get_pull_request(switch("2", "1", "1"))["data"]
    assert edited["title"] == "Foo"
    assert edited["body"] == "Blah blah blah."
    client.update_pull_request(switch("2", "1", "1"), title="Add blah")

    assert isinstance(client, CreatePullRequestProtocol)
    new_pull_request = client.create_pull_request(
        title=f"PR from API {now}",
        body="Another PR, made through the API.",
        head="topics/blih",
        base="main",
    )["data"]
    assert new_pull_request["body"] == "Another PR, made through the API."
    assert len(client.get_pull_requests()["data"]) == 2
    closed = client.update_pull_request(new_pull_request["id"], state="closed")["data"]
    assert closed["body"] == "Another PR, made through the API."
    assert len(client.get_pull_requests()["data"]) == 1
    closed = client.get_pull_request(new_pull_request["id"])["data"]
    assert closed["body"] == "Another PR, made through the API."


def test_create_pull_request_draft(service: str, switch: Switch, now: datetime, client: SourceCodeManager) -> None:
    assert isinstance(client, CreatePullRequestDraftProtocol)

    title = f"Draft PR from API {now}"
    result = client.create_pull_request_draft(
        title=title,
        body="A draft PR, made through the API.",
        head="topics/blih",
        base="main",
    )
    draft = result["data"]
    assert draft["body"] == "A draft PR, made through the API."
    assert draft["state"] == "open"
    # GitLab encodes draft-ness as a title prefix; GitHub and Bitbucket use a flag.
    assert draft["title"] == switch(title, f"Draft: {title}", title)
    if service in ("github", "bitbucket"):
        assert result["raw"]["data"]["draft"] is True

    # Clean up the pull request created while recording.
    assert isinstance(client, UpdatePullRequestProtocol)
    client.update_pull_request(draft["id"], state="closed")


def test_get_closed_pull_requests(switch: Switch, client: SourceCodeManager) -> None:
    assert isinstance(client, GetPullRequestsProtocol)

    closed_pull_requests = client.get_pull_requests(state="closed")["data"]
    closed_pull_request = next(pr for pr in closed_pull_requests if pr["id"] == switch("58", "103", "5"))
    assert closed_pull_request == {
        "id": switch("58", "103", "5"),
        "internal_id": switch("3998333440", "503797513", "5"),
        "title": switch(
            "PR from API 2026-07-06 13:26:59.552768",
            "PR from API 2026-07-06 12:36:59.203296",
            "PR from API 2026-07-06 12:37:05.636104",
        ),
        "author": {
            "id": switch("261902604", "150871", "{b116ba0e-54b0-48c1-8118-77624b8c8d33}"),
            "username": switch("sentry-integration-dev-jacquev6[bot]", "jacquev6", "jacquev6"),
        },
        "body": "Another PR, made through the API.",
        "state": "closed",
        "merged": False,
        "html_url": switch(
            "https://github.com/jacquev6/test-Sentry-Integration-Dev-jacquev6/pull/58",
            "https://gitlab.com/jacquev6-sentry/test-sentry-integration-dev-jacquev6/-/merge_requests/103",
            "https://bitbucket.org/jacquev6-sentry/test-sentry-integration-dev-jacquev6/pull-requests/5",
        ),
        "head": {
            "sha": switch(
                "6d8ca33dae268d3c5835e721e5702ef9dcb43c8c", "6d8ca33dae268d3c5835e721e5702ef9dcb43c8c", "6d8ca33dae26"
            ),
            "ref": "topics/blih",
        },
        "base": {
            "sha": switch("0941ee0a9eac9914cfddf5adec7a9558a2f1c447", None, "0941ee0a9eac"),
            "ref": "main",
        },
    }


def test_get_commits(switch: Switch, client: SourceCodeManager) -> None:
    assert isinstance(client, GetCommitsProtocol)

    assert client.get_commits(ref="1403774c82d64068af027d0b5d0cc4f52473b6f2")["data"] == [
        {
            "id": "1403774c82d64068af027d0b5d0cc4f52473b6f2",
            "message": "Initial commit",
            "author": {
                "name": "Vincent Jacques",
                "email": "vincent@vincent-jacques.net",
                "date": datetime.fromisoformat("2026-02-16T14:24:18+01:00"),
            },
            "additions": switch(None, 2, None),
            "deletions": switch(None, 0, None),
        }
    ]

    assert isinstance(client, GetCommitsByPathProtocol)

    commits = client.get_commits_by_path(path="README.md", ref="1403774c82d64068af027d0b5d0cc4f52473b6f2")

    assert commits["data"] == [
        {
            "id": "1403774c82d64068af027d0b5d0cc4f52473b6f2",
            "message": "Initial commit",
            "author": {
                "name": "Vincent Jacques",
                "email": "vincent@vincent-jacques.net",
                "date": datetime.fromisoformat("2026-02-16T14:24:18+01:00"),
            },
            "additions": switch(None, 2, None),
            "deletions": switch(None, 0, None),
        }
    ]

    assert client.get_commits_by_path(path="README.md")["data"] == [
        {
            "id": "1403774c82d64068af027d0b5d0cc4f52473b6f2",
            "message": "Initial commit",
            "author": {
                "name": "Vincent Jacques",
                "email": "vincent@vincent-jacques.net",
                "date": datetime.fromisoformat("2026-02-16T14:24:18+01:00"),
            },
            "additions": switch(None, 2, None),
            "deletions": switch(None, 0, None),
        }
    ]


def test_issue_comments(service: str, switch: Switch, client: SourceCodeManager) -> None:
    assert isinstance(client, GetIssueCommentsProtocol) == (service in ["github", "gitlab"])
    assert isinstance(client, CreateIssueCommentProtocol) == (service in ["github", "gitlab"])
    assert isinstance(client, DeleteIssueCommentProtocol) == (service in ["github", "gitlab"])

    if (
        isinstance(client, GetIssueCommentsProtocol)
        and isinstance(client, CreateIssueCommentProtocol)
        and isinstance(client, DeleteIssueCommentProtocol)
    ):
        assert client.get_issue_comments("1")["data"] == [
            {
                "id": switch("3983150774", "3123861269", None),
                "body": "A comment!",
                "author": {"id": switch("327146", "150871", None), "username": "jacquev6"},
                "author_association": switch("OWNER", None, None),
                "created_at": switch("2026-03-02T09:24:27Z", "2026-03-02T09:24:34.040Z", None),
                **switch(
                    {
                        "reactions": [
                            {
                                "author": None,
                                "content": "+1",
                                "id": "",
                            },
                            {
                                "author": None,
                                "content": "-1",
                                "id": "",
                            },
                            {
                                "author": None,
                                "content": "laugh",
                                "id": "",
                            },
                            {
                                "author": None,
                                "content": "confused",
                                "id": "",
                            },
                            {
                                "author": None,
                                "content": "heart",
                                "id": "",
                            },
                            {
                                "author": None,
                                "content": "hooray",
                                "id": "",
                            },
                            {
                                "author": None,
                                "content": "eyes",
                                "id": "",
                            },
                        ],
                    },
                    {},
                    {},
                ),
            }
        ]
        new_comment = client.create_issue_comment(issue_id="1", body="Another comment, made through the API.")["data"]
        assert new_comment["body"] == "Another comment, made through the API."
        assert len(client.get_issue_comments("1")["data"]) == 2
        client.delete_issue_comment(issue_id="1", comment_id=new_comment["id"])
        assert len(client.get_issue_comments("1")["data"]) == 1


def test_issue_comment_reactions(service: str, switch: Switch, client: SourceCodeManager) -> None:
    assert isinstance(client, GetIssueCommentReactionsProtocol) == (service in ["github", "gitlab"])
    assert isinstance(client, CreateIssueCommentReactionProtocol) == (service in ["github", "gitlab"])
    assert isinstance(client, DeleteIssueCommentReactionProtocol) == (service in ["github", "gitlab"])

    if (
        isinstance(client, GetIssueCommentReactionsProtocol)
        and isinstance(client, CreateIssueCommentReactionProtocol)
        and isinstance(client, DeleteIssueCommentReactionProtocol)
    ):
        author = {"id": switch("327146", "150871", None), "username": "jacquev6"}
        comment_id = switch("3983150774", "3123861269", None)
        reactions = client.get_issue_comment_reactions(issue_id="1", comment_id=comment_id)
        assert reactions["data"] == [
            {
                "id": switch("334443540", "43909506", None),
                "content": "+1",
                "author": author,
            },
            {
                "id": switch("334443546", "43909515", None),
                "content": "eyes",
                "author": author,
            },
            {
                "id": switch("334450300", "43911188", None),
                "content": "-1",
                "author": author,
            },
            {
                "id": switch("334450310", "43911265", None),
                "content": "laugh",
                "author": author,
            },
            {
                "id": switch("334450319", "43911283", None),
                "content": "hooray",
                "author": author,
            },
            {
                "id": switch("334450331", "43911304", None),
                "content": "confused",
                "author": author,
            },
            {
                "id": switch("334450342", "43911321", None),
                "content": "heart",
                "author": author,
            },
        ]
        # One GitLab emoji is not mapped, so it's dropped silently
        assert len(reactions["raw"]["data"]) == switch(7, 8, None)
        new_reaction = client.create_issue_comment_reaction(issue_id="1", comment_id=comment_id, reaction="rocket")[
            "data"
        ]
        assert new_reaction["content"] == "rocket"
        assert len(client.get_issue_comment_reactions(issue_id="1", comment_id=comment_id)["data"]) == 8
        client.delete_issue_comment_reaction(issue_id="1", comment_id=comment_id, reaction_id=new_reaction["id"])
        assert len(client.get_issue_comment_reactions(issue_id="1", comment_id=comment_id)["data"]) == 7


def test_pull_request_comments(switch: Switch, client: SourceCodeManager) -> None:
    assert isinstance(client, GetPullRequestCommentsProtocol)

    pull_request_id = switch("2", "1", "1")
    assert client.get_pull_request_comments(pull_request_id)["data"] == [
        {
            "id": switch("3983417927", "3537342606", "823492210"),
            "created_at": switch(
                "2026-03-02T10:13:38Z", "2026-07-08T14:21:06.369Z", "2026-07-08T14:49:13.528616+00:00"
            ),
            "body": "A great comment!",
            "author": {
                "id": switch("327146", "150871", "{b116ba0e-54b0-48c1-8118-77624b8c8d33}"),
                "username": "jacquev6",
            },
            "author_association": switch("OWNER", None, None),
            **switch(
                {
                    "reactions": [
                        {
                            "author": None,
                            "content": "+1",
                            "id": "",
                        },
                    ],
                },
                {},
                {},
            ),
        }
    ]

    assert isinstance(client, CreatePullRequestCommentProtocol)
    assert isinstance(client, DeletePullRequestCommentProtocol)
    new_comment = client.create_pull_request_comment(
        pull_request_id=pull_request_id, body="Another comment, made through the API."
    )["data"]
    assert new_comment["body"] == "Another comment, made through the API."
    assert len(client.get_pull_request_comments(pull_request_id)["data"]) == 2
    client.delete_pull_request_comment(pull_request_id=pull_request_id, comment_id=new_comment["id"])
    assert len(client.get_pull_request_comments(pull_request_id)["data"]) == 1


def test_pull_request_comment_reactions(service: str, switch: Switch, client: SourceCodeManager) -> None:
    assert isinstance(client, GetPullRequestCommentReactionsProtocol) == (service in ["github", "gitlab"])
    assert isinstance(client, CreatePullRequestCommentReactionProtocol) == (service in ["github", "gitlab"])
    assert isinstance(client, DeletePullRequestCommentReactionProtocol) == (service in ["github", "gitlab"])

    if (
        isinstance(client, GetPullRequestCommentReactionsProtocol)
        and isinstance(client, CreatePullRequestCommentReactionProtocol)
        and isinstance(client, DeletePullRequestCommentReactionProtocol)
    ):
        pull_request_id = switch("2", "1", None)
        comment_id = switch("3983417927", "3124015530", None)
        reactions = client.get_pull_request_comment_reactions(pull_request_id=pull_request_id, comment_id=comment_id)
        assert reactions["data"] == [
            {
                "id": switch("334495774", "43921665", None),
                "content": "+1",
                "author": {"id": switch("327146", "150871", None), "username": "jacquev6"},
            }
        ]
        # One GitLab emoji is not mapped, so it's dropped silently
        assert len(reactions["raw"]["data"]) == switch(1, 2, None)
        new_reaction = client.create_pull_request_comment_reaction(
            pull_request_id=pull_request_id, comment_id=comment_id, reaction="rocket"
        )["data"]
        assert new_reaction["content"] == "rocket"
        assert (
            len(
                client.get_pull_request_comment_reactions(pull_request_id=pull_request_id, comment_id=comment_id)[
                    "data"
                ]
            )
            == 2
        )
        client.delete_pull_request_comment_reaction(
            pull_request_id=pull_request_id,
            comment_id=comment_id,
            reaction_id=new_reaction["id"],
        )
        assert (
            len(
                client.get_pull_request_comment_reactions(pull_request_id=pull_request_id, comment_id=comment_id)[
                    "data"
                ]
            )
            == 1
        )


def test_issue_reactions(service: str, switch: Switch, client: SourceCodeManager) -> None:
    assert isinstance(client, GetIssueReactionsProtocol) == (service in ["github", "gitlab"])
    assert isinstance(client, CreateIssueReactionProtocol) == (service in ["github", "gitlab"])
    assert isinstance(client, DeleteIssueReactionProtocol) == (service in ["github", "gitlab"])

    if (
        isinstance(client, GetIssueReactionsProtocol)
        and isinstance(client, CreateIssueReactionProtocol)
        and isinstance(client, DeleteIssueReactionProtocol)
    ):
        issue_id = "1"
        reactions = client.get_issue_reactions(issue_id)
        assert reactions["data"] == [
            {
                "id": switch("277533978", "43923647", None),
                "content": "+1",
                "author": {"id": switch("327146", "150871", None), "username": "jacquev6"},
            },
            {
                "id": switch("277533995", "43923674", None),
                "content": "hooray",
                "author": {"id": switch("327146", "150871", None), "username": "jacquev6"},
            },
        ]
        # One GitLab emoji is not mapped, so it's dropped silently
        assert len(reactions["raw"]["data"]) == switch(2, 3, None)
        new_reaction = client.create_issue_reaction(issue_id=issue_id, reaction="rocket")["data"]
        assert new_reaction["content"] == "rocket"
        assert len(client.get_issue_reactions(issue_id)["data"]) == 3
        client.delete_issue_reaction(issue_id=issue_id, reaction_id=new_reaction["id"])
        assert len(client.get_issue_reactions(issue_id)["data"]) == 2


def test_pull_request_reactions(service: str, switch: Switch, client: SourceCodeManager) -> None:
    assert isinstance(client, GetPullRequestReactionsProtocol) == (service in ["github", "gitlab"])
    assert isinstance(client, CreatePullRequestReactionProtocol) == (service in ["github", "gitlab"])
    assert isinstance(client, DeletePullRequestReactionProtocol) == (service in ["github", "gitlab"])

    if (
        isinstance(client, GetPullRequestReactionsProtocol)
        and isinstance(client, CreatePullRequestReactionProtocol)
        and isinstance(client, DeletePullRequestReactionProtocol)
    ):
        pull_request_id = switch("2", "1", None)
        reactions = client.get_pull_request_reactions(pull_request_id)
        assert reactions["data"] == [
            {
                "id": switch("277538935", "43924243", None),
                "content": "-1",
                "author": {"id": switch("327146", "150871", None), "username": "jacquev6"},
            }
        ]
        # One GitLab emoji is not mapped, so it's dropped silently
        assert len(reactions["raw"]["data"]) == switch(1, 2, None)
        new_reaction = client.create_pull_request_reaction(pull_request_id=pull_request_id, reaction="rocket")["data"]
        assert new_reaction["content"] == "rocket"
        assert len(client.get_pull_request_reactions(pull_request_id)["data"]) == 2
        client.delete_pull_request_reaction(pull_request_id=pull_request_id, reaction_id=new_reaction["id"])
        assert len(client.get_pull_request_reactions(pull_request_id)["data"]) == 1


def test_branches(service: str, now: datetime, client: SourceCodeManager) -> None:
    assert isinstance(client, GetBranchProtocol)

    assert client.get_branch(branch="topics/blah")["data"] == {
        "ref": "topics/blah",
        "sha": "7497e018d01503b6abc3053b7896266115e631f6",
    }

    assert isinstance(client, CreateBranchProtocol)
    assert isinstance(client, DeleteBranchProtocol)
    branch = now.strftime("tests/%Y%m%d-%H%M%S")
    assert client.create_branch(branch=branch, sha="0941ee0a9eac9914cfddf5adec7a9558a2f1c447")["data"] == {
        "ref": branch,
        "sha": "0941ee0a9eac9914cfddf5adec7a9558a2f1c447",
    }
    assert client.get_branch(branch=branch)["data"] == {
        "ref": branch,
        "sha": "0941ee0a9eac9914cfddf5adec7a9558a2f1c447",
    }
    assert isinstance(client, UpdateBranchProtocol) == (service == "github")
    if isinstance(client, UpdateBranchProtocol):
        assert client.update_branch(branch=branch, sha="6d8ca33dae268d3c5835e721e5702ef9dcb43c8c")["data"] == {
            "ref": branch,
            "sha": "6d8ca33dae268d3c5835e721e5702ef9dcb43c8c",
        }
        assert client.get_branch(branch=branch)["data"] == {
            "ref": branch,
            "sha": "6d8ca33dae268d3c5835e721e5702ef9dcb43c8c",
        }
    client.delete_branch(branch=branch)
    with pytest.raises(SCMError):
        client.get_branch(branch=branch)


def test_file_content(switch: Switch, client: SourceCodeManager) -> None:
    assert isinstance(client, GetFileContentProtocol)

    github_content = (
        "IyB0ZXN0LVNlbnRyeS1JbnRlZ3JhdGlvbi1EZXYtamFjcXVldjYKVGVzdCBy\n"
        + "ZXBvIGZvciBteSBkZXZlbG9wbWVudHMgaW4gU2VudHJ5J3MgR2l0SHViIEFw\ncAo=\n"
    )
    gitlab_content = github_content.replace("\n", "")
    assert (
        base64.b64decode(github_content).decode("utf-8")
        == "# test-Sentry-Integration-Dev-jacquev6\nTest repo for my developments in Sentry's GitHub App\n"
    )
    assert client.get_file_content(path="README.md", ref="main")["data"] == {
        "content": switch(github_content, gitlab_content, gitlab_content),
        "encoding": "base64",
        "path": "README.md",
        "sha": "d96986775b6793cac0a358b35650de94752a9530",
        "size": 92,
        "type": "file",
    }

    assert client.get_file_content(path="BLAH.md", ref="topics/blah")["data"] == {
        "content": "MQoyCjMKNAo1CjYKNwo4CjkK" + switch("\n", "", ""),
        "encoding": "base64",
        "path": "BLAH.md",
        "sha": "07193989308c972f8a2d0f1b3a15c29ea4ac565b",
        "size": 18,
        "type": "file",
    }


def test_get_tree(switch: Switch, client: SourceCodeManager) -> None:
    assert isinstance(client, GetTreeProtocol)

    # A commit SHA of the `topics/subdirs` branch, which nests a few directories.
    # Commit SHAs are identical across the three repos (pushed from the same objects).
    commit = "4a7da3cfe581f2a96233cd6dbb16a76472cb0a6c"

    def entry(path: str, mode: str, type_: str, sha: str, size: int | None) -> dict[str, Any]:
        # git object SHAs are content-addressed, so GitHub and GitLab agree; Bitbucket's
        # /src listing exposes none, so it is empty. GitLab never reports a size.
        return {
            "path": path,
            "mode": mode,
            "type": type_,
            "sha": switch(sha, sha, ""),
            "size": switch(size, None, size),
        }

    # Order differs across providers (GitHub/GitLab git order vs Bitbucket breadth-first),
    # so compare sorted by path.
    def by_path(tree: Sequence[Any]) -> list[Any]:
        return sorted(tree, key=lambda e: e["path"])

    expected_full_tree = [
        entry("README.md", "100644", "blob", "d96986775b6793cac0a358b35650de94752a9530", 92),
        entry("README2.md", "100644", "blob", "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391", 0),
        entry("a", "040000", "tree", "5105eb153744b1240e9a6494aad9005c98c1c7f8", None),
        entry("a/b", "040000", "tree", "cf8b3eaec626e780d80b482cb376893cab399509", None),
        entry("a/b/A.md", "100644", "blob", "f70f10e4db19068f79bc43844b49f3eece45c4e8", 2),
        entry("a/b/D.md", "100644", "blob", "178481050188cf00d7d9cd5a11e43ab8fab9294f", 2),
        entry("a/c", "040000", "tree", "62028f2714488ab682456129c47e7656f70708b2", None),
        entry("a/c/B.md", "100644", "blob", "223b7836fb19fdf64ba2d3cd6173c6a283141f78", 2),
        entry("d", "040000", "tree", "2b7cb3bfc4e26e6cab55d6febac511e03da7272a", None),
        entry("d/C.md", "100644", "blob", "3cc58df83752123644fef39faab2393af643b1d2", 2),
        entry("d/E.md", "100644", "blob", "1c507261389e25abfe3620ddd348c73f4eb3b91e", 2),
    ]

    # get_tree returns a single page. GitHub sends the whole tree at once, but GitLab
    # and Bitbucket paginate (Bitbucket's /src default is only 10 per page), so ask for
    # a page large enough to hold the whole tree everywhere and get truncated=False.
    recursive = client.get_tree(commit, pagination={"per_page": 100})["data"]
    assert recursive["sha"] == commit
    assert recursive["truncated"] is False
    assert by_path(recursive["tree"]) == expected_full_tree

    # get_full_tree walks every page and returns the whole tree in one (non-paginated) result.
    assert isinstance(client, GetFullTreeProtocol)
    full = client.get_full_tree(commit)["data"]
    assert full["sha"] == commit
    assert full["truncated"] is False
    assert by_path(full["tree"]) == expected_full_tree

    # Without recursion, only the direct children of the root are listed.
    root = client.get_tree(commit, recursive=False)["data"]
    assert root["sha"] == commit
    assert by_path(root["tree"]) == [
        entry("README.md", "100644", "blob", "d96986775b6793cac0a358b35650de94752a9530", 92),
        entry("README2.md", "100644", "blob", "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391", 0),
        entry("a", "040000", "tree", "5105eb153744b1240e9a6494aad9005c98c1c7f8", None),
        entry("d", "040000", "tree", "2b7cb3bfc4e26e6cab55d6febac511e03da7272a", None),
    ]


def test_get_directory_contents(switch: Switch, client: SourceCodeManager) -> None:
    assert isinstance(client, GetDirectoryContentsProtocol)

    ref = "topics/subdirs"

    def entry(path: str, type_: str, sha: str, size: int) -> dict[str, Any]:
        # A listing carries no content; sha is a git object id on GitHub/GitLab but empty
        # on Bitbucket (/src exposes none), and GitLab reports size 0 for every entry.
        return {
            "path": path,
            "type": type_,
            "sha": switch(sha, sha, ""),
            "content": "",
            "encoding": "",
            "size": switch(size, 0, size),
        }

    def by_path(entries: Sequence[Any]) -> list[Any]:
        return sorted(entries, key=lambda e: e["path"])

    # A directory of subdirectories.
    assert by_path(client.get_directory_contents("a", ref=ref)["data"]) == [
        entry("a/b", "directory", "cf8b3eaec626e780d80b482cb376893cab399509", 0),
        entry("a/c", "directory", "62028f2714488ab682456129c47e7656f70708b2", 0),
    ]

    # A directory of files.
    assert by_path(client.get_directory_contents("d", ref=ref)["data"]) == [
        entry("d/C.md", "file", "3cc58df83752123644fef39faab2393af643b1d2", 2),
        entry("d/E.md", "file", "1c507261389e25abfe3620ddd348c73f4eb3b91e", 2),
    ]

    # Pointing at a file (not a directory) is an error on all three providers.
    with pytest.raises(PathIsNotDirectory):
        client.get_directory_contents("README.md", ref=ref)


def test_get_readme(switch: Switch, client: SourceCodeManager) -> None:
    assert isinstance(client, GetReadmeProtocol)

    # Same README.md as test_file_content: GitHub's base64 is newline-wrapped, the others aren't.
    github_content = (
        "IyB0ZXN0LVNlbnRyeS1JbnRlZ3JhdGlvbi1EZXYtamFjcXVldjYKVGVzdCBy\n"
        + "ZXBvIGZvciBteSBkZXZlbG9wbWVudHMgaW4gU2VudHJ5J3MgR2l0SHViIEFw\ncAo=\n"
    )
    gitlab_content = github_content.replace("\n", "")
    assert client.get_readme(ref="main")["data"] == {
        "content": switch(github_content, gitlab_content, gitlab_content),
        "encoding": "base64",
        "path": "README.md",
        "sha": "d96986775b6793cac0a358b35650de94752a9530",
        "size": 92,
        "type": "file",
    }


def test_create_commit(service: str, switch: Switch, client: SourceCodeManager) -> None:
    assert isinstance(client, CreateCommitProtocol)
    assert isinstance(client, GetGitCommitProtocol)
    assert isinstance(client, GetCommitChangesProtocol)

    # Force-push a pre-existing throwaway branch so the test is repeatable across
    # re-recordings; parent_sha is the base commit shared by all three repos.
    base = "0941ee0a9eac9914cfddf5adec7a9558a2f1c447"
    branch = "topics/create-commit-test"
    message = "e2e: create two files"
    created = client.create_commit(
        branch,
        base,
        message,
        [
            WriteCommitAction(action="create", filename="e2e/one.txt", content="one\n", encoding="utf-8"),
            WriteCommitAction(action="create", filename="e2e/two.txt", content="two\n", encoding="utf-8"),
        ],
        force=True,
    )["data"]
    sha = created["id"]
    # Bitbucket echoes the commit message with a trailing newline; GitHub/GitLab don't.
    expected_message = switch(message, message, message + "\n")
    assert created["message"] == expected_message

    # get_git_commit reads the same commit back as a git object.
    git_commit = client.get_git_commit(sha)["data"]
    assert git_commit["sha"] == sha
    assert git_commit["message"] == expected_message
    # GitHub returns the real tree-object SHA; GitLab and Bitbucket echo the commit SHA.
    if service == "github":
        assert git_commit["tree"]["sha"]
    else:
        assert git_commit["tree"]["sha"] == sha

    # get_commit_changes reports the files the commit added (against its parent).
    changes = client.get_commit_changes(sha)["data"]
    assert sorted((f["filename"], f["status"], f["additions"], f["deletions"]) for f in changes) == [
        ("e2e/one.txt", "added", 1, 0),
        ("e2e/two.txt", "added", 1, 0),
    ]


def test_compare_commits(switch: Switch, client: SourceCodeManager) -> None:
    assert isinstance(client, CompareCommitsProtocol)

    assert client.compare_commits(
        start_sha="0941ee0a9eac9914cfddf5adec7a9558a2f1c447",
        end_sha="6d8ca33dae268d3c5835e721e5702ef9dcb43c8c",
    )["data"] == {
        "ahead_by": 1,
        **switch(
            {
                "behind_by": 0,
            },
            {},
            {},
        ),
        "commits": [
            {
                "id": "6d8ca33dae268d3c5835e721e5702ef9dcb43c8c",
                "message": switch("Add blah", "Add blah\n", "Add blah\n"),
                "author": {
                    "name": "Vincent Jacques",
                    "email": "vincent@vincent-jacques.net",
                    "date": datetime.fromisoformat("2026-02-26T08:47:45Z"),
                },
                "additions": None,
                "deletions": None,
            }
        ],
        "diff": [
            {
                "additions": 0,
                "deletions": 0,
                "filename": "BLAH.md",
                "patch": switch(None, "", None),
                "previous_filename": None,
                "status": "added",
            },
        ],
    }


def test_get_commit(switch: Switch, client: SourceCodeManager) -> None:
    assert isinstance(client, GetCommitProtocol)

    assert client.get_commit(sha="6d8ca33dae268d3c5835e721e5702ef9dcb43c8c")["data"] == {
        "id": "6d8ca33dae268d3c5835e721e5702ef9dcb43c8c",
        "message": switch("Add blah", "Add blah\n", "Add blah\n"),
        "additions": switch(0, 0, None),
        "deletions": switch(0, 0, None),
        "author": {
            "name": "Vincent Jacques",
            "email": "vincent@vincent-jacques.net",
            "date": datetime.fromisoformat("2026-02-26T08:47:45Z"),
        },
        "files": switch(
            [
                {
                    "filename": "BLAH.md",
                    "status": "added",
                    "patch": None,
                    "additions": 0,
                    "deletions": 0,
                    "previous_filename": None,
                }
            ],
            None,
            None,
        ),
    }


def test_get_pull_request_files(switch: Switch, client: SourceCodeManager) -> None:
    assert isinstance(client, GetPullRequestFilesProtocol)

    pull_request_id = switch("2", "1", "1")
    assert client.get_pull_request_files(pull_request_id)["data"] == [
        {
            "changes": switch(9, 9, 9),
            "filename": "BLAH.md",
            "previous_filename": None,
            "sha": switch("07193989308c972f8a2d0f1b3a15c29ea4ac565b", "", ""),
            "status": "added",
            "patch": switch(
                "@@ -0,0 +1,9 @@\n+1\n+2\n+3\n+4\n+5\n+6\n+7\n+8\n+9",
                "@@ -0,0 +1,9 @@\n+1\n+2\n+3\n+4\n+5\n+6\n+7\n+8\n+9\n",
                None,
            ),
        }
    ]

    # The raw unified diff of the same PR.
    assert isinstance(client, GetPullRequestDiffProtocol)
    diff = client.get_pull_request_diff(pull_request_id)["data"]
    assert "BLAH.md" in diff
    assert "+1\n+2\n+3\n+4\n+5\n+6\n+7\n+8\n+9" in diff


def test_get_pull_request_commits(switch: Switch, client: SourceCodeManager) -> None:
    assert isinstance(client, GetPullRequestCommitsProtocol)

    pull_request_id = switch("2", "1", "1")
    assert client.get_pull_request_commits(pull_request_id)["data"] == [
        {
            "sha": "6d8ca33dae268d3c5835e721e5702ef9dcb43c8c",
            "message": switch("Add blah", "Add blah\n", "Add blah\n"),
            "author": {
                "name": "Vincent Jacques",
                "email": "vincent@vincent-jacques.net",
                "date": datetime.fromisoformat("2026-02-26T08:47:45Z"),
            },
        },
        {
            "sha": "7497e018d01503b6abc3053b7896266115e631f6",
            "message": switch("Add content", "Add content\n", "Add content\n"),
            "author": {
                "name": "Vincent Jacques",
                "email": "vincent@vincent-jacques.net",
                "date": datetime.fromisoformat("2026-03-05T11:15:50Z"),
            },
        },
    ]


def test_get_pull_request_template(service: str, switch: Switch, client: SourceCodeManager) -> None:
    assert isinstance(client, GetPullRequestTemplateProtocol)
    # Each repo carries a single PR template at the provider's conventional path.
    templates = list(client.get_pull_request_template(ref="topics/templates"))
    assert len(templates) == 1
    template = templates[0]["data"]
    assert template["path"] == switch(
        ".github/pull_request_template.md",
        ".gitlab/merge_request_templates/Default.md",
        ".bitbucket/pull_request_template.md",
    )
    assert base64.b64decode(template["content"]).decode() == f"Template for {service}.\n"


def test_review_comments(service: str, switch: Switch, now: datetime, client: SourceCodeManager) -> None:
    pull_request_id = switch("2", "1", "1")

    assert isinstance(client, CreateReviewCommentFileProtocol)
    body = f"A review comment, on a file, made by the API on {now}."
    comment_on_file = client.create_review_comment_file(
        pull_request_id=pull_request_id,
        commit_id="7497e018d01503b6abc3053b7896266115e631f6",
        body=body,
        path="BLAH.md",
        side="head",
    )
    # We don't have getters for review comments, so our checks are limited
    assert isinstance(comment_on_file["data"]["id"], str)
    assert comment_on_file["data"]["body"] == body

    assert isinstance(client, CreateReviewCommentReplyProtocol)
    body = f"A reply to the previous comment, made by the API on {now}."
    reply_comment = client.create_review_comment_reply(
        pull_request_id=pull_request_id,
        body=body,
        comment_id=comment_on_file["data"]["id"],
    )
    assert isinstance(reply_comment["data"]["id"], str)
    assert reply_comment["data"]["body"] == body

    assert isinstance(client, CreateReviewCommentLineProtocol)
    body = f"A review comment, on a line, made by the API on {now}."
    comment_on_line = client.create_review_comment_line(
        pull_request_id=pull_request_id,
        commit_id="7497e018d01503b6abc3053b7896266115e631f6",
        body=body,
        path="BLAH.md",
        line=5,
        side="head",
    )
    assert isinstance(comment_on_line["data"]["id"], str)
    assert comment_on_line["data"]["body"] == body
    body = f"A reply to the previous line comment, made by the API on {now}."
    reply_comment = client.create_review_comment_reply(
        pull_request_id=pull_request_id,
        body=body,
        comment_id=comment_on_line["data"]["id"],
    )
    assert isinstance(reply_comment["data"]["id"], str)
    assert reply_comment["data"]["body"] == body

    assert isinstance(client, CreateReviewCommentMultilineProtocol) == (service in ["github", "gitlab"])
    if isinstance(client, CreateReviewCommentMultilineProtocol):
        body = f"A review comment, on multiple lines, made by the API on {now}."
        comment_on_multiline = client.create_review_comment_multiline(
            pull_request_id=pull_request_id,
            commit_id="7497e018d01503b6abc3053b7896266115e631f6",
            body=body,
            path="BLAH.md",
            start_line=5,
            end_line=7,
            start_side="head",
            side="head",
        )
        assert isinstance(comment_on_multiline["data"]["id"], str)
        assert comment_on_multiline["data"]["body"] == body
        body = f"A reply to the previous multiline comment, made by the API on {now}."
        reply_comment = client.create_review_comment_reply(
            pull_request_id=pull_request_id,
            body=body,
            comment_id=comment_on_multiline["data"]["id"],
        )
        assert isinstance(reply_comment["data"]["id"], str)
        assert reply_comment["data"]["body"] == body

    # Edit a comment in place.
    assert isinstance(client, UpdateReviewCommentProtocol)
    updated_body = f"An edited review comment, made by the API on {now}."
    updated = client.update_review_comment(pull_request_id, comment_on_file["data"]["id"], updated_body)
    assert updated["data"]["body"] == updated_body

    # A third top-level comment, so each resolve action below gets its own thread:
    # Bitbucket rejects re-resolving an already-resolved thread (409), so no thread
    # may be resolved twice.
    body = f"A review comment to collapse, made by the API on {now}."
    comment_to_collapse = client.create_review_comment_file(
        pull_request_id=pull_request_id,
        commit_id="7497e018d01503b6abc3053b7896266115e631f6",
        body=body,
        path="BLAH.md",
        side="head",
    )

    # Map each comment to its thread id: Bitbucket/GitLab derive it locally from the
    # comment id, GitHub looks it up -- all behind one method.
    assert isinstance(client, ResolveReviewThreadProtocol)
    file_thread_id = client.get_thread_id_from_review_comment_unique_id(
        pull_request_id, comment_on_file["data"]["unique_id"] or ""
    )
    collapse_thread_id = client.get_thread_id_from_review_comment_unique_id(
        pull_request_id, comment_to_collapse["data"]["unique_id"] or ""
    )
    line_thread_id = client.get_thread_id_from_review_comment_unique_id(
        pull_request_id, comment_on_line["data"]["unique_id"] or ""
    )
    assert file_thread_id is not None and collapse_thread_id is not None and line_thread_id is not None

    # Resolve one thread directly...
    client.resolve_review_thread(pull_request_id, file_thread_id)

    # ...collapse another via the higher-level entry point...
    assert isinstance(client, CollapsePullRequestCommentProtocol)
    client.collapse_pull_request_comment(
        pull_request_id, collapse_thread_id, comment_to_collapse["data"]["unique_id"] or ""
    )

    # ...and edit-and-collapse the last one.
    assert isinstance(client, UpdateAndCollapsePullRequestCommentProtocol)
    edited_body = f"An edited, then collapsed, review comment, made by the API on {now}."
    edited = client.update_and_collapse_pull_request_comment(
        pull_request_id=pull_request_id,
        thread_id=line_thread_id,
        comment_id=comment_on_line["data"]["id"],
        comment_node_id=comment_on_line["data"]["unique_id"] or "",
        body=edited_body,
    )
    assert edited["data"]["body"] == edited_body

    # Read the threads back. The PR accumulates comments across recordings, so we look up
    # the specific threads created above by id rather than asserting the whole set; and since
    # each provider returns a single page, walk every page to collect them all.
    assert isinstance(client, GetPullRequestReviewThreadsProtocol)
    threads: dict[str, Any] = {}
    pagination: PaginationParams = {"per_page": 100}
    while True:
        page = client.get_pull_request_review_threads(pull_request_id, pagination=pagination)
        threads.update({t["id"]: t for t in page["data"]})
        next_cursor = page["meta"]["next_cursor"]
        if not next_cursor:
            break
        pagination = {"per_page": 100, "cursor": next_cursor}

    # The line-level thread: resolved (edit-and-collapse), anchored to line 5.
    line_thread = threads[line_thread_id]
    assert line_thread["is_resolved"] is True
    assert line_thread["file_path"] == "BLAH.md"
    assert line_thread["line"] == 5
    assert len(line_thread["comments"]) >= 2
    assert edited_body in [c["body"] for c in line_thread["comments"]]

    # The file-level thread: resolved, on BLAH.md, holding the edited comment and its reply.
    # GitLab only surfaces line-anchored discussions as review threads, so it omits this one.
    if service != "gitlab":
        file_thread = threads[file_thread_id]
        assert file_thread["is_resolved"] is True
        assert file_thread["file_path"] == "BLAH.md"
        assert len(file_thread["comments"]) >= 2
        assert updated_body in [c["body"] for c in file_thread["comments"]]


def test_create_review(switch: Switch, now: datetime, client: SourceCodeManager) -> None:
    assert isinstance(client, CreateReviewProtocol)

    pull_request_id = switch("2", "1", "1")
    body = f"A review, made by the API on {now}."
    review = client.create_review(
        pull_request_id=pull_request_id,
        commit_sha="7497e018d01503b6abc3053b7896266115e631f6",
        event="comment",
        comments=[
            {
                "path": "BLAH.md",
                "body": f"A review comment on a line, made by the API on {now}.",
                "line": 5,
                "side": "head",
            }
        ],
        body=body,
    )["data"]
    assert review == {
        "id": review["id"],
        "html_url": switch(
            f"https://github.com/jacquev6/test-Sentry-Integration-Dev-jacquev6/pull/2#pullrequestreview-{review['id']}",
            "https://gitlab.com/jacquev6-sentry/test-sentry-integration-dev-jacquev6/-/merge_requests/1",
            "https://bitbucket.org/jacquev6-sentry/test-sentry-integration-dev-jacquev6/pull-requests/1",
        ),
        **switch(
            {
                "author": {
                    "id": "261902604",
                    "username": "sentry-integration-dev-jacquev6[bot]",
                },
                "body": body,
                "commit_id": "7497e018d01503b6abc3053b7896266115e631f6",
                "state": "commented",
                "submitted_at": review.get("submitted_at"),
            },
            {},
            {},
        ),
    }


def test_check_runs(switch: Switch, now: datetime, client: SourceCodeManager) -> None:
    assert isinstance(client, CreateCheckRunProtocol)
    assert isinstance(client, GetCheckRunProtocol)
    assert isinstance(client, UpdateCheckRunProtocol)

    name = f"Created via API {now.strftime('%Y%m%d %H%M%S')}"

    check_run = client.create_check_run(
        name=name,
        head_sha="7497e018d01503b6abc3053b7896266115e631f6",
        status="pending",
        conclusion=None,
    )["data"]
    check_run_id = check_run["id"]
    check_run_url = switch(
        f"https://github.com/jacquev6/test-Sentry-Integration-Dev-jacquev6/runs/{check_run_id}",
        "https://gitlab.com/jacquev6-sentry/test-sentry-integration-dev-jacquev6/-/commit/7497e018d01503b6abc3053b7896266115e631f6",
        "https://bitbucket.org/jacquev6-sentry/test-sentry-integration-dev-jacquev6/commits/7497e018d01503b6abc3053b7896266115e631f6",
    )
    assert check_run == {
        "id": check_run_id,
        "name": name,
        "html_url": check_run_url,
        "status": switch("pending", "pending", "running"),
        "conclusion": None,
    }

    assert client.get_check_run(check_run_id)["data"] == {
        "id": check_run_id,
        "name": name,
        "html_url": check_run_url,
        "status": switch("pending", "pending", "running"),
        "conclusion": None,
    }

    assert client.update_check_run(
        check_run_id,
        status="completed",
        conclusion="success",
        output={
            "title": "Output set by API",
            "summary": "Set by API",
            "text": "This output was set through the API",
        },
    )["data"] == {
        "id": check_run_id,
        "name": name,
        "html_url": check_run_url,
        "status": "completed",
        "conclusion": "success",
    }

    assert client.get_check_run(check_run_id)["data"] == {
        "id": check_run_id,
        "name": name,
        "html_url": check_run_url,
        "status": "completed",
        "conclusion": "success",
    }
