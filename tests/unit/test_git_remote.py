import pytest

from scm.errors import GitProviderNotSupported, InvalidWebBaseUrl
from scm.git.remote import git_remote
from scm.git.types import GitRemote
from tests.unit.git_builders import make_repository


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


@pytest.mark.parametrize("provider_name", ["github_enterprise", "gitlab"])
def test_self_hosted_providers_need_a_web_base_url(provider_name):
    with pytest.raises(InvalidWebBaseUrl, match=f"{provider_name} repository has no web_base_url"):
        git_remote(make_repository(provider_name=provider_name))


@pytest.mark.parametrize(
    "web_base_url",
    ["http://ghe.example.com", "ghe.example.com", "https://", "https://ghe.example.com?x=1", ""],
)
@pytest.mark.parametrize("provider_name", ["github_enterprise", "gitlab"])
def test_web_base_url_must_be_https(provider_name, web_base_url):
    repository = make_repository(provider_name=provider_name, web_base_url=web_base_url)
    with pytest.raises(InvalidWebBaseUrl, match="not an https URL"):
        git_remote(repository)


@pytest.mark.parametrize(
    "web_base_url,url",
    [
        ("https://gitlab.com", "https://gitlab.com/group/sub/project.git"),
        ("https://git.example.com:8443/", "https://git.example.com:8443/group/sub/project.git"),
        ("https://example.com/gitlab", "https://example.com/gitlab/group/sub/project.git"),
    ],
)
def test_gitlab(web_base_url, url):
    repository = make_repository(
        provider_name="gitlab", external_id="gitlab.com:42", name="group / sub / project", web_base_url=web_base_url
    )
    assert git_remote(repository) == GitRemote(url=url, username="oauth2")


@pytest.mark.parametrize("provider_name", ["bitbucket", "cursor_origin"])
def test_unsupported_providers(provider_name):
    with pytest.raises(GitProviderNotSupported, match=provider_name):
        git_remote(make_repository(provider_name=provider_name))
