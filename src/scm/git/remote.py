from urllib.parse import urlsplit

from scm.errors import GitProviderNotSupported, InvalidWebBaseUrl, MalformedExternalId
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
            if repository["web_base_url"] is None:
                raise InvalidWebBaseUrl(detail="The GitHub Enterprise repository has no web_base_url.")
            base_url = _https_base_url(repository["web_base_url"])
            return GitRemote(url=f"{base_url}/{repository['name']}.git", username="x-access-token")
        case "gitlab":
            # Sentry's repository name for GitLab is the namespace path with spaces around each slash.
            path = repository["name"].replace(" ", "")
            return GitRemote(url=f"{_gitlab_base_url(repository)}/{path}.git", username="oauth2")
        case provider_name:
            raise GitProviderNotSupported(detail=f"The {provider_name} provider has no git remote.")


def _gitlab_base_url(repository: Repository) -> str:
    if repository["web_base_url"] is not None:
        return _https_base_url(repository["web_base_url"])
    # Sentry does not set web_base_url for GitLab yet. Until it does, take the host from the external id,
    # "<host>:<project id>", and assume https, as GitLabProvider does. That loses an instance served under a
    # path prefix, which only web_base_url carries.
    external_id = repository["external_id"]
    if external_id is None or ":" not in external_id:
        raise MalformedExternalId()
    return f"https://{external_id.rsplit(':', maxsplit=1)[0]}"


def _https_base_url(web_base_url: str) -> str:
    # The token goes to this URL, so it must be https.
    parts = urlsplit(web_base_url)
    if parts.scheme != "https" or not parts.netloc or parts.query or parts.fragment:
        raise InvalidWebBaseUrl(detail=f"The repository's web_base_url {web_base_url!r} is not an https URL.")
    return web_base_url.rstrip("/")
