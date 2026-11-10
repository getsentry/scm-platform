"""Pushes built to slip past the push check, each of which it must refuse."""

import io
import time

import pytest

from scm.errors import GitMalformedRequestBody, GitOperationNotGranted
from scm.git.push import MAX_UPDATES_LENGTH, PushRefused, check_push
from scm.git.relay import GitRelay
from scm.git.types import GrantV2PushRule
from tests.unit.git_builders import NEW_ID, OLD_ID, make_push_body, make_repository, pkt
from tests.unit.git_upstream import running_upstream
from tests.unit.test_git_server import PUSH_GRANT, Recorder, make_server, request

RULES = [GrantV2PushRule(repository=5, branches=["seer/fix"])]


def first_update(capabilities: bytes, ref: bytes = b"refs/heads/seer/fix") -> bytes:
    return pkt(f"{OLD_ID} {NEW_ID} ".encode() + ref + b"\0" + capabilities)


class TestCapabilityValues:
    @pytest.mark.parametrize(
        "capabilities",
        [
            b"report-status agent=git/2.54.0\0push-options",
            b"report-status session-id=x\0push-options",
            b"report-status agent=git/2.54.0\x01push-options",
            b"report-status session-id=\x1b[2Jx",
        ],
        ids=["nul-in-agent", "nul-in-session-id", "control-in-agent", "escape-in-session-id"],
    )
    def test_value_git_would_not_send_is_refused(self, capabilities):
        # git sends capability values as printable ASCII. A NUL or control byte in an allowed one can hide a
        # capability such as push-options from the check, while a receive-pack that reads the whole line sees it.
        body = first_update(capabilities) + b"0000PACK"
        with pytest.raises((GitOperationNotGranted, GitMalformedRequestBody)):
            check_push(RULES, {}, io.BytesIO(body))


def long_capability_push() -> bytes:
    """A push whose one unsupported capability takes half the update budget, followed by as many updates as fit."""
    lines = [first_update(b"report-status " + b"x" * (MAX_UPDATES_LENGTH // 2), ref=b"refs/heads/r0")]
    index = 1
    while (
        sum(map(len, lines)) + len(line := pkt(f"{OLD_ID} {NEW_ID} refs/heads/r{index}".encode())) + 4
        <= MAX_UPDATES_LENGTH
    ):
        lines.append(line)
        index += 1
    return b"".join(lines) + b"0000"


class TestRefusalSize:
    def test_report_does_not_repeat_a_long_capability_name_per_update(self):
        # Every refused ref's "ng" line repeats the reason, so a reason that quoted a long capability whole would
        # answer a small push with megabytes.
        body = long_capability_push()
        with pytest.raises(PushRefused) as exc_info:
            check_push(RULES, {}, io.BytesIO(body))
        assert exc_info.value.report is not None
        assert len(exc_info.value.report) <= 2 * len(body)

    def test_detail_cuts_a_long_capability_name_short(self):
        # The client chooses the capability's name, and the detail reaches logs and Sentry issues.
        with pytest.raises(PushRefused) as exc_info:
            check_push(RULES, {}, io.BytesIO(long_capability_push()))
        assert len(exc_info.value.detail) <= 1000


class TestRefusalDetail:
    @pytest.mark.parametrize(
        "ref",
        [b"refs/tags/x\nERROR forged log line", b"refs/tags/x\x1b[2J", b"refs/pull/1\rhead"],
        ids=["newline", "escape", "carriage-return"],
    )
    def test_detail_holds_no_control_characters(self, ref):
        # git refuses such a name as a "funny refname" and never sends one, and the detail reaches logs.
        body = first_update(b"report-status", ref=ref) + b"0000"
        with pytest.raises((GitOperationNotGranted, GitMalformedRequestBody)) as exc_info:
            check_push(RULES, {}, io.BytesIO(body))
        detail = exc_info.value.detail
        assert not any(ord(character) < 0x20 or ord(character) == 0x7F for character in detail), detail


class TestPatternCost:
    def test_long_branch_name_does_not_pin_the_worker(self):
        # An ordinary-looking pattern with three wildcards is cubic in the name's length: 1,500 characters take
        # seconds, and 60,000 would take days.
        rules = [GrantV2PushRule(repository=5, branch_patterns=["seer/.+-.+-.+-[0-9]+"])]
        body = make_push_body((OLD_ID, NEW_ID, "refs/heads/seer/" + "-" * 1500 + "x"))
        started = time.perf_counter()
        with pytest.raises((GitOperationNotGranted, GitMalformedRequestBody)):
            check_push(rules, {}, io.BytesIO(body))
        assert time.perf_counter() - started < 0.25


class TestHeaders:
    def test_second_content_encoding_header_is_not_sent_upstream_unchecked(self):
        # A mapping can hold both spellings and the relay passes on only one, so a body checked as plain could go
        # upstream marked gzip.
        body = make_push_body((OLD_ID, NEW_ID, "refs/heads/seer/fix"))
        with running_upstream() as upstream:
            recorder = Recorder(
                repository=make_repository(provider_name="github_enterprise", web_base_url=upstream.url)
            )
            request(
                make_server(recorder, GitRelay(ca_bundle=upstream.ca_bundle)),
                PUSH_GRANT,
                method="POST",
                route="git-receive-pack",
                query="",
                headers={"Content-Encoding": "identity", "content-encoding": "gzip"},
                body=io.BytesIO(body),
            )
        assert [r.headers.get("Content-Encoding") for r in upstream.requests] in ([], ["identity"])

    def test_git_protocol_git_does_not_send_on_a_push(self):
        # git sends no Git-Protocol header on a push, or version=1, since a push has no protocol v2.
        body = make_push_body((OLD_ID, NEW_ID, "refs/heads/seer/fix"))
        with running_upstream() as upstream:
            recorder = Recorder(
                repository=make_repository(provider_name="github_enterprise", web_base_url=upstream.url)
            )
            request(
                make_server(recorder, GitRelay(ca_bundle=upstream.ca_bundle)),
                PUSH_GRANT,
                method="POST",
                route="git-receive-pack",
                query="",
                headers={"Git-Protocol": "version=2:future-key=value"},
                body=io.BytesIO(body),
            )
        assert all(r.headers.get("Git-Protocol") in (None, "version=0", "version=1") for r in upstream.requests)
