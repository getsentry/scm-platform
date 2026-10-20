from scm.errors import GitProviderNotSupported, MalformedExternalId
from scm.git.types import GitRemote
from scm.types import Repository

GITHUB_GIT_BASE_URL = "https://github.com"


def git_remote(repository: Repository) -> GitRemote:
    """Return the git endpoint and basic-auth username for a repository's provider.

    Bitbucket Cloud (``x-token-auth``) and Azure DevOps (a bearer header) follow the same pattern once the
    platform supports them as providers.
    """
    match repository["provider_name"]:
        case "github":
            return GitRemote(url=f"{GITHUB_GIT_BASE_URL}/{repository['name']}.git", username="x-access-token")
        case "github_enterprise":
            if not repository["web_base_url"]:
                raise GitProviderNotSupported(detail="The GitHub Enterprise repository has no web_base_url.")
            base_url = repository["web_base_url"].rstrip("/")
            return GitRemote(url=f"{base_url}/{repository['name']}.git", username="x-access-token")
        case "gitlab":
            # Sentry leaves web_base_url unset for GitLab; the host is in the external id, "<netloc>:<project id>",
            # and the name is the namespace path with spaces around its slashes. GitLabProvider reads both the same
            # way.
            external_id = repository["external_id"]
            if external_id is None or ":" not in external_id:
                raise MalformedExternalId()
            netloc = external_id.rsplit(":", maxsplit=1)[0]
            path = repository["name"].replace(" ", "")
            return GitRemote(url=f"https://{netloc}/{path}.git", username="oauth2")
        case provider_name:
            raise GitProviderNotSupported(detail=f"The {provider_name} provider has no git remote.")
