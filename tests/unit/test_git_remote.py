import pytest

from scm.errors import GitProviderNotSupported, MalformedExternalId
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


def test_github_enterprise_without_host():
    with pytest.raises(GitProviderNotSupported, match="no web_base_url"):
        git_remote(make_repository(provider_name="github_enterprise"))


def test_gitlab():
    repository = make_repository(provider_name="gitlab", external_id="gitlab.com:42", name="group / sub / project")
    assert git_remote(repository) == GitRemote(url="https://gitlab.com/group/sub/project.git", username="oauth2")


def test_self_hosted_gitlab_with_port():
    repository = make_repository(provider_name="gitlab", external_id="git.example.com:8443:42", name="team / app")
    assert git_remote(repository) == GitRemote(url="https://git.example.com:8443/team/app.git", username="oauth2")


@pytest.mark.parametrize("external_id", [None, "42"])
def test_gitlab_malformed_external_id(external_id):
    with pytest.raises(MalformedExternalId):
        git_remote(make_repository(provider_name="gitlab", external_id=external_id))


@pytest.mark.parametrize("provider_name", ["bitbucket", "cursor_origin"])
def test_unsupported_providers(provider_name):
    with pytest.raises(GitProviderNotSupported, match=provider_name):
        git_remote(make_repository(provider_name=provider_name))
