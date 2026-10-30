"""Builders shared by the git module's tests."""

from typing import NamedTuple
from unittest.mock import MagicMock

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat, load_pem_private_key

from scm.git.grant import generate_private_key
from scm.git.types import Grant
from scm.providers.cursor_origin.provider import CursorOriginProvider
from scm.providers.github.provider import GitHubProvider
from scm.providers.gitlab.provider import GitLabProvider
from scm.secret import Secret
from scm.types import ApiClient, Provider, Repository

# Unix seconds long after any test runs, for grants a test doesn't expect to expire.
FAR_FUTURE = 2**40


def make_repository(**overrides) -> Repository:
    defaults: Repository = {
        "id": 5,
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


def make_provider(organization_id: int, repository: Repository) -> Provider | None:
    """Build the repository's provider the way Sentry's ``fetch_provider`` does, with a client nothing calls."""
    client = MagicMock(spec=ApiClient)
    match repository["provider_name"]:
        case "github" | "github_enterprise":
            return GitHubProvider(client, organization_id, repository)
        case "gitlab":
            return GitLabProvider(client, organization_id, repository)
        case "cursor_origin":
            return CursorOriginProvider(client, organization_id, repository, installation_id="1")
        case _:
            return None


def make_grant(**overrides) -> Grant:
    """A fetch grant for the default repository, named both ways a grant can name it."""
    fields = {
        "organization_id": 1,
        "referrer": "seer.coding_job",
        "operation": "fetch",
        "repositories": [5, ("github", "123")],
        "expires_at": FAR_FUTURE,
    }
    fields.update(overrides)
    return Grant(**fields)  # type: ignore[arg-type]


class KeyPair(NamedTuple):
    key: Ed25519PrivateKey
    """The private key, to sign tokens sign_grant would not."""
    private: Secret
    """The private key in PEM, as sign_grant takes it."""
    public: str
    """The public key in PEM, as verify_grant and GitServer take it."""


def make_key_pair() -> KeyPair:
    """A new Ed25519 key pair for signing grants. Tests make their own, so no private key is committed."""
    private = generate_private_key()
    key = load_pem_private_key(private.reveal().encode(), password=None)
    assert isinstance(key, Ed25519PrivateKey)
    return KeyPair(
        key=key,
        private=private,
        # Serialized here rather than by public_key_of, so the tests check public_key_of against cryptography.
        public=key.public_key().public_bytes(Encoding.PEM, PublicFormat.SubjectPublicKeyInfo).decode(),
    )
