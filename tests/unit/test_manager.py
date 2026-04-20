import pytest

from scm.errors import SCMCodedError
from scm.facade import Facade
from scm.manager import SourceCodeManager
from scm.test_fixtures import BaseTestProvider
from scm.types import Repository


def make_repository(**overrides) -> Repository:
    defaults: Repository = {
        "id": 1,
        "external_id": "abc123",
        "integration_id": 1,
        "is_active": True,
        "name": "test-repo",
        "organization_id": 1,
        "provider_name": "github",
    }
    return {**defaults, **overrides}  # type: ignore[typeddict-item]


def mock_record_count(a, b, c):
    return None


class TestMakeFromRepositoryId:
    def test_make_from_repository_id(self):
        repo = make_repository()
        provider = BaseTestProvider()

        result = SourceCodeManager.make_from_repository_id(
            organization_id=1,
            repository_id=1,
            referrer="test-referrer",
            fetch_repository=lambda org, rid: repo,
            fetch_provider=lambda org, r: provider,
            record_count=mock_record_count,
        )

        assert isinstance(result, Facade)
        assert result.provider is provider
        assert result.referrer == "test-referrer"
        assert result.record_count is mock_record_count
        assert hasattr(result, "get_branch")
        assert hasattr(result, "create_pull_request_comment")

    def test_default_referrer_is_shared(self):
        repo = make_repository()
        provider = BaseTestProvider()

        result = SourceCodeManager.make_from_repository_id(
            organization_id=1,
            repository_id=1,
            fetch_repository=lambda org, rid: repo,
            fetch_provider=lambda org, r: provider,
            record_count=mock_record_count,
        )

        assert result.referrer == "shared"

    def test_repository_not_found_raises(self):

        with pytest.raises(SCMCodedError, check=lambda e: e.code == "repository_not_found"):
            SourceCodeManager.make_from_repository_id(
                organization_id=1,
                repository_id=1,
                fetch_repository=lambda org, rid: None,
                fetch_provider=lambda org, r: BaseTestProvider(),
                record_count=mock_record_count,
            )

    def test_inactive_repository_raises(self):
        repo = make_repository(is_active=False)

        with pytest.raises(SCMCodedError, check=lambda e: e.code == "repository_inactive"):
            SourceCodeManager.make_from_repository_id(
                organization_id=1,
                repository_id=1,
                fetch_repository=lambda org, rid: repo,
                fetch_provider=lambda org, r: BaseTestProvider(),
                record_count=mock_record_count,
            )

    def test_organization_mismatch_raises(self):
        repo = make_repository(organization_id=999)

        with pytest.raises(SCMCodedError, check=lambda e: e.code == "repository_organization_mismatch"):
            SourceCodeManager.make_from_repository_id(
                organization_id=1,
                repository_id=1,
                fetch_repository=lambda org, rid: repo,
                fetch_provider=lambda org, r: BaseTestProvider(),
                record_count=mock_record_count,
            )

    def test_provider_not_found_raises(self):
        repo = make_repository()

        with pytest.raises(SCMCodedError, check=lambda e: e.code == "provider_not_found"):
            SourceCodeManager.make_from_repository_id(
                organization_id=1,
                repository_id=1,
                fetch_repository=lambda org, rid: repo,
                fetch_provider=lambda org, r: None,
                record_count=mock_record_count,
            )

    def test_with_tuple_repository_id(self):
        repo = make_repository()
        provider = BaseTestProvider()

        result = SourceCodeManager.make_from_repository_id(
            organization_id=1,
            repository_id=("github", "abc123"),
            fetch_repository=lambda org, rid: repo,
            fetch_provider=lambda org, r: provider,
            record_count=mock_record_count,
        )

        assert isinstance(result, Facade)
