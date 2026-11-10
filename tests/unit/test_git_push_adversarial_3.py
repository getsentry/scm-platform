"""Tag pushes built to slip past the push check, each of which it must refuse."""

import io

import pytest

from scm.git.push import PushRefused, check_push
from scm.git.types import GrantV2PushRule
from tests.unit.git_builders import NEW_ID, ZERO_ID, make_push_body


class TestPatternDigits:
    @pytest.mark.parametrize(
        ("field", "ref"),
        [
            ("tag_patterns", "refs/tags/v１.０"),
            ("tag_patterns", "refs/tags/v١.٠"),
            ("branch_patterns", "refs/heads/v１.０"),
        ],
        ids=["fullwidth-tag", "arabic-indic-tag", "fullwidth-branch"],
    )
    def test_digit_class_does_not_cover_a_lookalike_of_a_release(self, field, ref):
        # Without re.ASCII, \d matches every Unicode decimal digit, so a rule written for v1.0 would let its holder
        # create a tag that renders like v1.0 and is a different ref.
        rules = [GrantV2PushRule(repository=5, create_only=True, **{field: [r"v\d+\.\d+"]})]
        with pytest.raises(PushRefused):
            check_push(rules, {}, io.BytesIO(make_push_body((ZERO_ID, NEW_ID, ref))))
