import pytest

from scm.errors import GitProviderNotSupported, InvalidWebBaseUrl, MalformedExternalId
from scm.git.remote import git_remote
from scm.git.types import GitRemote
from scm.types import Repository


def make_repository(**overrides) -> Repository:
    defaults: Repository = {
        "id": 1,
        "external_id": "123",
        "integration_id": 1,
        "is_active": True,
        "name": "getsentry/sentry",
        "organization_id": 1,
        "provider_name": "github",
        "web_base_url": None,
        "installation_id": None,
    }
    return {**defaults, **overrides}  # type: ignore[typeddict-item]


def test_github():
    assert git_remote(make_repository()) == GitRemote(
        url="https://github.com/getsentry/sentry.git", username="x-access-token"
    )


@pytest.mark.parametrize("web_base_url", ["https://ghe.example.com", "https://ghe.example.com/"])
def test_github_enterprise(web_base_url):
    repository = make_repository(provider_name="github_enterprise", web_base_url=web_base_url)
    assert git_remote(repository) == GitRemote(
        url="https://ghe.example.com/getsentry/sentry.git", username="x-access-token"
    )


def test_github_enterprise_without_web_base_url():
    with pytest.raises(InvalidWebBaseUrl, match="no web_base_url"):
        git_remote(make_repository(provider_name="github_enterprise"))


@pytest.mark.parametrize(
    "web_base_url",
    ["http://ghe.example.com", "ghe.example.com", "https://", "https://ghe.example.com?x=1", ""],
)
@pytest.mark.parametrize("provider_name", ["github_enterprise", "gitlab"])
def test_web_base_url_must_be_https(provider_name, web_base_url):
    repository = make_repository(provider_name=provider_name, external_id="gitlab.com:42", web_base_url=web_base_url)
    with pytest.raises(InvalidWebBaseUrl, match="not an https URL"):
        git_remote(repository)


@pytest.mark.parametrize(
    "web_base_url,url",
    [
        ("https://git.example.com", "https://git.example.com/team/app.git"),
        ("https://example.com/gitlab/", "https://example.com/gitlab/team/app.git"),
    ],
)
def test_gitlab_prefers_web_base_url(web_base_url, url):
    repository = make_repository(
        provider_name="gitlab", external_id="ignored.example.com:42", name="team / app", web_base_url=web_base_url
    )
    assert git_remote(repository) == GitRemote(url=url, username="oauth2")


def test_gitlab():
    repository = make_repository(provider_name="gitlab", external_id="gitlab.com:42", name="group / sub / project")
    assert git_remote(repository) == GitRemote(url="https://gitlab.com/group/sub/project.git", username="oauth2")


def test_self_hosted_gitlab_with_port():
    repository = make_repository(provider_name="gitlab", external_id="git.example.com:8443:42", name="team / app")
    assert git_remote(repository) == GitRemote(url="https://git.example.com:8443/team/app.git", username="oauth2")


@pytest.mark.parametrize("external_id", [None, "42"])
def test_gitlab_without_web_base_url_needs_a_host_in_the_external_id(external_id):
    with pytest.raises(MalformedExternalId):
        git_remote(make_repository(provider_name="gitlab", external_id=external_id))


@pytest.mark.parametrize("provider_name", ["bitbucket", "cursor_origin"])
def test_unsupported_providers(provider_name):
    with pytest.raises(GitProviderNotSupported, match=provider_name):
        git_remote(make_repository(provider_name=provider_name))
