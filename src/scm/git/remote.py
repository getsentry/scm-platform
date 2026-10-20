from urllib.parse import urlsplit

from scm.errors import GitProviderNotSupported, InvalidWebBaseUrl
from scm.git.types import GitRemote
from scm.types import Repository

GITHUB_GIT_BASE_URL = "https://github.com"


def git_remote(repository: Repository) -> GitRemote:
    """Return the git endpoint and basic-auth username for a repository's provider.

    Bitbucket Cloud (basic auth as ``x-token-auth``) can be added the same way once the platform has a provider
    for it. Azure DevOps authenticates git with a bearer header, which ``GitRemote`` cannot express yet.
    """
    match repository["provider_name"]:
        case "github":
            return GitRemote(url=f"{GITHUB_GIT_BASE_URL}/{repository['name']}.git", username="x-access-token")
        case "github_enterprise":
            base_url = _https_base_url(repository)
            return GitRemote(url=f"{base_url}/{repository['name']}.git", username="x-access-token")
        case "gitlab":
            # Sentry's repository name for GitLab is the namespace path with spaces around each slash.
            path = repository["name"].replace(" ", "")
            return GitRemote(url=f"{_https_base_url(repository)}/{path}.git", username="oauth2")
        case provider_name:
            raise GitProviderNotSupported(detail=f"The {provider_name} provider has no git remote.")


def _https_base_url(repository: Repository) -> str:
    # The token goes to this URL, so it must be https. Sentry sets web_base_url for every GitHub Enterprise and
    # GitLab repository from the integration's instance URL; for GitLab that keeps the path prefix of an instance
    # served under one.
    web_base_url = repository["web_base_url"]
    if web_base_url is None:
        raise InvalidWebBaseUrl(detail=f"The {repository['provider_name']} repository has no web_base_url.")
    parts = urlsplit(web_base_url)
    if parts.scheme != "https" or not parts.netloc or parts.query or parts.fragment:
        raise InvalidWebBaseUrl(detail=f"The repository's web_base_url {web_base_url!r} is not an https URL.")
    return web_base_url.rstrip("/")
