import io

import pytest

from scm.errors import ClientClosedRequest, GitMalformedRequestBody, GitOperationNotGranted, RpcMalformedRequestHeaders
from scm.git.push import (
    MAX_UPDATES_LENGTH,
    PushRefused,
    PushRequest,
    RefUpdate,
    ReplayedBody,
    check_push,
    check_push_headers,
    read_push_request,
    refusal_report,
    refused_updates,
)
from scm.git.types import PushRule
from tests.unit.git_builders import NEW_ID, OLD_ID, PACK, ZERO_ID, make_push_body, pkt

RULES = [PushRule(repository=5, branches=["seer/fix", "seer/other"])]


def read(body: bytes, headers: dict[str, str] | None = None) -> tuple[PushRequest, io.BytesIO]:
    stream = io.BytesIO(body)
    return read_push_request(headers or {}, stream), stream


class Trickle(io.BytesIO):
    """A body that returns at most one byte per read, as a socket may."""

    def read(self, size: int | None = -1, /) -> bytes:
        return super().read(1 if size is None or size < 0 else min(size, 1))


class Failing:
    def read(self, size: int = -1, /) -> bytes:
        raise ConnectionResetError()


class TestRead:
    def test_updates_and_capabilities(self):
        body = make_push_body((OLD_ID, NEW_ID, "refs/heads/seer/fix"), (ZERO_ID, NEW_ID, "refs/heads/seer/other"))
        request, stream = read(body)
        assert request.updates == [
            RefUpdate(old_id=OLD_ID.encode(), new_id=NEW_ID.encode(), ref=b"refs/heads/seer/fix"),
            RefUpdate(old_id=ZERO_ID.encode(), new_id=NEW_ID.encode(), ref=b"refs/heads/seer/other"),
        ]
        assert request.capabilities == {
            "report-status",
            "side-band-64k",
            "quiet",
            "object-format=sha1",
            "agent=git/2.54.0",
        }

    def test_leaves_the_pack_unread(self):
        body = make_push_body((OLD_ID, NEW_ID, "refs/heads/seer/fix"))
        request, stream = read(body)
        assert request.head + stream.read() == body
        assert stream.getvalue()[len(request.head) :] == PACK

    def test_probe(self):
        # git sends a lone flush before a push too large to buffer, to check its credentials before it streams.
        request, stream = read(b"0000")
        assert request == PushRequest(updates=[], capabilities=frozenset(), head=b"0000")

    def test_shallow_commits(self):
        body = make_push_body((OLD_ID, NEW_ID, "refs/heads/seer/fix"), shallow=("3" * 40, "4" * 40))
        request, stream = read(body)
        assert [update.ref for update in request.updates] == [b"refs/heads/seer/fix"]
        assert request.head + stream.read() == body

    def test_sha256_object_ids(self):
        old_id, new_id = "a" * 64, "b" * 64
        body = make_push_body(
            (old_id, new_id, "refs/heads/seer/fix"), capabilities="report-status object-format=sha256"
        )
        request, _ = read(body)
        assert request.updates[0].new_id == new_id.encode()

    @pytest.mark.parametrize(
        "capabilities,ids,shallow",
        [
            ("report-status object-format=sha256", ("1" * 40, "2" * 40), ()),
            ("report-status object-format=sha1", ("a" * 64, "b" * 64), ()),
            ("report-status", ("a" * 64, "b" * 64), ()),
            ("report-status", (OLD_ID, NEW_ID), ("3" * 64,)),
            ("report-status object-format=md5", (OLD_ID, NEW_ID), ()),
            ("report-status object-format=sha1 object-format=sha256", (OLD_ID, NEW_ID), ()),
        ],
        ids=["sha256-with-sha1-ids", "sha1-with-sha256-ids", "default-with-sha256-ids", "shallow-length", "md5", "two"],
    )
    def test_object_ids_that_dont_match_the_object_format(self, capabilities, ids, shallow):
        body = make_push_body((*ids, "refs/heads/seer/fix"), capabilities=capabilities, shallow=shallow)
        with pytest.raises(GitMalformedRequestBody):
            read(body)

    def test_capabilities_on_a_later_update(self):
        # receive-pack reads capabilities after a NUL on any update, so one there would slip past the allowlist.
        first = f"{OLD_ID} {NEW_ID} refs/heads/seer/fix".encode()
        later = f"{OLD_ID} {NEW_ID} refs/heads/seer/other".encode()
        body = pkt(first + b"\0report-status") + pkt(later + b"\0push-options") + b"0000"
        with pytest.raises(GitMalformedRequestBody, match="first ref update may carry capabilities"):
            read(body)

    @pytest.mark.parametrize(
        "ref",
        [
            b"refs/heads/",
            b"refs/heads/a b",
            b"refs/heads/a..b",
            b"refs/heads/a\x01",
            b"refs/heads/x.lock",
            b"refs/heads/\xff",
        ],
    )
    def test_branch_name_git_would_not_take(self, ref):
        line = f"{OLD_ID} {NEW_ID} ".encode() + ref
        with pytest.raises(GitMalformedRequestBody, match="name git wouldn't take"):
            read(pkt(line + b"\0report-status") + b"0000")

    def test_same_ref_twice(self):
        body = make_push_body((OLD_ID, NEW_ID, "refs/heads/seer/fix"), ("2" * 40, "3" * 40, "refs/heads/seer/fix"))
        with pytest.raises(GitMalformedRequestBody, match="more than once"):
            read(body)

    def test_short_reads(self):
        body = make_push_body((OLD_ID, NEW_ID, "refs/heads/seer/fix"), shallow=("3" * 40,))
        request = read_push_request({}, Trickle(body))
        assert request.updates[0].ref == b"refs/heads/seer/fix"

    @pytest.mark.parametrize(
        "lines",
        [
            [b"\0report-status\n", b""],
            [b"\0report-status", b"\n"],
        ],
        ids=["first", "later"],
    )
    def test_update_ending_in_a_newline(self, lines):
        # git ends no update with a newline, though receive-pack would drop one.
        first = f"{OLD_ID} {NEW_ID} refs/heads/seer/fix".encode()
        later = f"{OLD_ID} {NEW_ID} refs/heads/seer/other".encode()
        with pytest.raises(GitMalformedRequestBody):
            read(pkt(first + lines[0]) + pkt(later + lines[1]) + b"0000")

    def test_shallow_commit_without_its_newline(self):
        body = pkt(f"shallow {'3' * 40}".encode()) + make_push_body((OLD_ID, NEW_ID, "refs/heads/seer/fix"))
        with pytest.raises(GitMalformedRequestBody, match="shallow commit"):
            read(body)

    def test_uppercase_pkt_length(self):
        # git writes the length in lowercase hex. 0x00ea is a length with a letter in it.
        line = f"{OLD_ID} {NEW_ID} refs/heads/{'x' * (0xEA - 4 - 93)}".encode()
        assert len(line) + 4 == 0xEA
        with pytest.raises(GitMalformedRequestBody, match="not pkt-lines"):
            read(b"%04X" % (len(line) + 4) + line + b"0000")

    @pytest.mark.parametrize("encoding", ["identity", "", "gzip", "deflate"])
    def test_content_encoding(self, encoding):
        # git sends no Content-Encoding with a push, and compresses only fetches.
        with pytest.raises(RpcMalformedRequestHeaders, match="content-encoding"):
            read(make_push_body((OLD_ID, NEW_ID, "refs/heads/seer/fix")), {"content-encoding": encoding})

    @pytest.mark.parametrize(
        "method,headers",
        [
            ("POST", {"Accept": "application/x-git-receive-pack-result", "Git-Protocol": "version=1"}),
            ("POST", {"Content-Type": "application/x-git-receive-pack-request"}),
            ("POST", {"User-Agent": "git/2.54.0", "Accept-Encoding": "deflate, gzip"}),
            ("GET", {"Accept": "*/*", "Git-Protocol": "version=1", "Pragma": "no-cache"}),
        ],
    )
    def test_headers_git_sends(self, method, headers):
        check_push_headers(method, headers)

    @pytest.mark.parametrize(
        "method,headers",
        [
            ("POST", {"Git-Protocol": "version=0"}),
            ("POST", {"Git-Protocol": "version=2"}),
            ("POST", {"Accept": "*/*"}),
            ("GET", {"Accept": "application/x-git-receive-pack-result"}),
            ("GET", {"Content-Type": "application/x-git-receive-pack-request"}),
            ("POST", {"User-Agent": " git/2.54.0"}),
            ("POST", {"Accept-Encoding": "gzip\x7f"}),
            ("POST", {"User-Agent": ""}),
        ],
    )
    def test_headers_git_does_not_send(self, method, headers):
        with pytest.raises(RpcMalformedRequestHeaders):
            check_push_headers(method, headers)

    def test_signed_push(self):
        body = pkt(b"push-cert\0report-status") + pkt(b"certificate version 0.1\n") + b"0000"
        with pytest.raises(GitOperationNotGranted, match="signed pushes"):
            read(body)

    @pytest.mark.parametrize(
        "body",
        [
            pytest.param(b"", id="empty"),
            pytest.param(b"00", id="short-length"),
            pytest.param(b"zzzz", id="length-not-hex"),
            pytest.param(b"+004", id="length-with-sign"),
            pytest.param(b" 0a\n", id="length-with-spaces"),
            pytest.param(b"0001", id="delimiter"),
            pytest.param(b"0003", id="v2-special-packet"),
            pytest.param(b"fff1" + b"x" * 65517, id="pkt-line-too-long"),
            pytest.param(pkt(f"{OLD_ID} {NEW_ID} refs/heads/a".encode()), id="no-flush"),
            pytest.param(pkt(f"{OLD_ID} {NEW_ID}".encode()) + b"0000", id="no-ref"),
            pytest.param(pkt(f"{OLD_ID} {NEW_ID} ".encode()) + b"0000", id="empty-ref"),
            pytest.param(pkt(f"{OLD_ID[:-1]} {NEW_ID} refs/heads/a".encode()) + b"0000", id="short-id"),
            pytest.param(pkt(f"{'A' * 40} {NEW_ID} refs/heads/a".encode()) + b"0000", id="uppercase-id"),
            pytest.param(pkt(f"{OLD_ID} {'b' * 64} refs/heads/a".encode()) + b"0000", id="mixed-id-lengths"),
            pytest.param(pkt(b"shallow 123\n") + b"0000", id="bad-shallow"),
            pytest.param(pkt(f"{OLD_ID} {NEW_ID} refs/heads/a\0caps \xff".encode("latin-1")) + b"0000", id="caps"),
        ],
    )
    def test_malformed(self, body):
        with pytest.raises(GitMalformedRequestBody):
            read(body)

    def test_shallow_line_after_an_update(self):
        body = make_push_body((OLD_ID, NEW_ID, "refs/heads/a"))
        flush = body.index(b"0000")
        with pytest.raises(GitMalformedRequestBody):
            read(body[:flush] + pkt(f"shallow {'3' * 40}".encode()) + body[flush:])

    def test_too_long_before_the_pack(self):
        updates = [(OLD_ID, NEW_ID, f"refs/heads/branch-{i:05d}") for i in range(MAX_UPDATES_LENGTH // 90)]
        with pytest.raises(GitMalformedRequestBody, match="too long"):
            read(make_push_body(*updates))

    def test_connection_fails(self):
        with pytest.raises(ClientClosedRequest):
            read_push_request({}, Failing())


def request_for(*updates: tuple[str, str, str], capabilities: str = "report-status side-band-64k") -> PushRequest:
    return read(make_push_body(*updates, capabilities=capabilities))[0]


class TestRefusedUpdates:
    def test_listed_branches(self):
        request = request_for((OLD_ID, NEW_ID, "refs/heads/seer/fix"), (ZERO_ID, NEW_ID, "refs/heads/seer/other"))
        assert refused_updates(RULES, request) == {}

    def test_force_push_to_a_listed_branch(self):
        # Nothing here knows whether the new commit descends from the old one, so the provider's protection decides.
        assert refused_updates(RULES, request_for(("3" * 40, NEW_ID, "refs/heads/seer/fix"))) == {}

    def test_probe(self):
        assert refused_updates(RULES, read(b"0000")[0]) == {}

    @pytest.mark.parametrize(
        "ref,reason",
        [
            ("refs/heads/main", "the git grant does not cover this branch"),
            ("refs/heads/seer/fix/deeper", "the git grant does not cover this branch"),
            ("refs/heads/seer", "the git grant does not cover this branch"),
            ("refs/heads/Seer/fix", "the git grant does not cover this branch"),
            ("refs/tags/seer/fix", "the git grant does not cover this tag"),
            ("refs/pull/1/head", "the git grant allows pushing branches and tags only"),
            ("refs/notes/commits", "the git grant allows pushing branches and tags only"),
        ],
    )
    def test_refs_the_rule_does_not_list(self, ref, reason):
        assert refused_updates(RULES, request_for((OLD_ID, NEW_ID, ref))) == {ref.encode(): reason}

    def test_deleting_a_listed_branch(self):
        refused = refused_updates(RULES, request_for((OLD_ID, ZERO_ID, "refs/heads/seer/fix")))
        assert refused == {b"refs/heads/seer/fix": "the git grant does not allow deleting a branch"}

    def test_one_refused_update_refuses_the_push(self):
        request = request_for((OLD_ID, NEW_ID, "refs/heads/seer/fix"), (OLD_ID, NEW_ID, "refs/heads/main"))
        assert refused_updates(RULES, request) == {
            b"refs/heads/main": "the git grant does not cover this branch",
            b"refs/heads/seer/fix": "refused with the rest of the push",
        }

    @pytest.mark.parametrize(
        "ref,reason",
        [
            ("refs/heads/seer/fix", "the git grant does not cover this branch"),
            ("refs/tags/v1", "the git grant does not cover this tag"),
            ("refs/notes/commits", "the git grant allows pushing branches and tags only"),
        ],
    )
    def test_no_rules(self, ref, reason):
        assert refused_updates([], request_for((OLD_ID, NEW_ID, ref))) == {ref.encode(): reason}

    @pytest.mark.parametrize(
        "old_id,ref,reason",
        [
            (ZERO_ID, "refs/heads/seer/new", None),
            (OLD_ID, "refs/heads/seer/new", "the git grant allows only creating this branch"),
            (ZERO_ID, "refs/heads/main", "the git grant does not cover this branch"),
            (ZERO_ID, "refs/tags/seer/new", "the git grant does not cover this tag"),
        ],
    )
    def test_create_only_pattern(self, old_id, ref, reason):
        rules = [PushRule(repository=5, branch_patterns=["seer/.+"], create_only=True)]
        refused = refused_updates(rules, request_for((old_id, NEW_ID, ref)))
        assert refused == ({} if reason is None else {ref.encode(): reason})

    def test_create_only_listed_branch(self):
        rules = [PushRule(repository=5, branches=["seer/fix"], create_only=True)]
        assert refused_updates(rules, request_for((ZERO_ID, NEW_ID, "refs/heads/seer/fix"))) == {}
        assert refused_updates(rules, request_for((OLD_ID, NEW_ID, "refs/heads/seer/fix"))) == {
            b"refs/heads/seer/fix": "the git grant allows only creating this branch"
        }

    @pytest.mark.parametrize(
        "old_id,new_id,ref,reason",
        [
            (ZERO_ID, NEW_ID, "refs/tags/v1.0", None),
            (OLD_ID, NEW_ID, "refs/tags/v1.0", "the git grant allows only creating this tag"),
            (OLD_ID, ZERO_ID, "refs/tags/v1.0", "the git grant does not allow deleting a tag"),
            (ZERO_ID, NEW_ID, "refs/tags/seer-1.0", "the git grant does not cover this tag"),
            (ZERO_ID, NEW_ID, "refs/heads/v1.0", "the git grant does not cover this branch"),
        ],
    )
    def test_create_only_tag_pattern(self, old_id, new_id, ref, reason):
        rules = [PushRule(repository=5, tag_patterns=[r"v[0-9]+\.[0-9]+"], create_only=True)]
        refused = refused_updates(rules, request_for((old_id, new_id, ref)))
        assert refused == ({} if reason is None else {ref.encode(): reason})

    def test_listed_tag_moves(self):
        # Without create_only a covered tag may move, as a covered branch may.
        rules = [PushRule(repository=5, tags=["latest"])]
        assert refused_updates(rules, request_for((OLD_ID, NEW_ID, "refs/tags/latest"))) == {}

    def test_one_rule_for_branches_and_tags(self):
        rules = [PushRule(repository=5, branch_patterns=["seer/.+"], tags=["seer-1"], create_only=True)]
        request = request_for((ZERO_ID, NEW_ID, "refs/heads/seer/fix"), (ZERO_ID, NEW_ID, "refs/tags/seer-1"))
        assert refused_updates(rules, request) == {}

    def test_any_rule_allows_an_update(self):
        # The agent may start branches under seer/ and keep pushing to the one Seer added after its first push.
        rules = [
            PushRule(repository=5, branch_patterns=["seer/.+"], create_only=True),
            PushRule(repository=5, branches=["seer/fix"]),
        ]
        request = request_for((OLD_ID, NEW_ID, "refs/heads/seer/fix"), (ZERO_ID, NEW_ID, "refs/heads/seer/next"))
        assert refused_updates(rules, request) == {}
        assert refused_updates(list(reversed(rules)), request) == {}

    def test_reason_of_the_rule_the_update_came_closest_to_passing(self):
        rules = [
            PushRule(repository=5, branches=["other"]),
            PushRule(repository=5, branch_patterns=["seer/.+"], create_only=True),
        ]
        assert refused_updates(rules, request_for((OLD_ID, NEW_ID, "refs/heads/seer/old"))) == {
            b"refs/heads/seer/old": "the git grant allows only creating this branch"
        }
        assert refused_updates(rules, request_for((OLD_ID, ZERO_ID, "refs/heads/seer/old"))) == {
            b"refs/heads/seer/old": "the git grant does not allow deleting a branch"
        }

    @pytest.mark.parametrize(
        "capabilities",
        [
            "report-status side-band-64k quiet object-format=sha1 agent=git/2.54.0",
            "report-status-v2 side-band-64k atomic session-id=abc",
            "",
        ],
    )
    def test_capabilities_git_sends(self, capabilities):
        assert (
            refused_updates(RULES, request_for((OLD_ID, NEW_ID, "refs/heads/seer/fix"), capabilities=capabilities))
            == {}
        )

    @pytest.mark.parametrize(
        "capability,reason",
        [
            ("future-capability", "the git endpoint does not relay the future-capability capability"),
            ("quiet=1", "the git endpoint does not relay the quiet capability"),
            # git asks for neither in a push.
            ("side-band", "the git endpoint does not relay the side-band capability"),
            ("ofs-delta", "the git endpoint does not relay the ofs-delta capability"),
            ("agent", "the git endpoint does not relay the agent capability"),
        ],
    )
    def test_capability_git_does_not_send(self, capability, reason):
        # Anything unknown could change what the push does, so it refuses the push.
        request = request_for((OLD_ID, NEW_ID, "refs/heads/seer/fix"), capabilities=f"report-status {capability}")
        assert refused_updates(RULES, request) == {b"refs/heads/seer/fix": reason}

    def test_push_options(self):
        request = request_for((OLD_ID, NEW_ID, "refs/heads/seer/fix"), capabilities="report-status push-options")
        assert refused_updates(RULES, request) == {
            b"refs/heads/seer/fix": "the git endpoint does not relay push options"
        }


def unband(response: bytes) -> bytes:
    """The band 1 data of a side band response, checking that it ends with a flush."""
    data, position = b"", 0
    while response[position : position + 4] != b"0000":
        length = int(response[position : position + 4], 16)
        assert response[position + 4] == 1
        data += response[position + 5 : position + length]
        position += length
    assert position + 4 == len(response)
    return data


class TestRefusalReport:
    REFUSED = {b"refs/heads/main": "the git grant does not cover this branch"}
    REPORT = pkt(b"unpack ok\n") + pkt(b"ng refs/heads/main the git grant does not cover this branch\n") + b"0000"

    def test_report_status(self):
        request = request_for((OLD_ID, NEW_ID, "refs/heads/main"), capabilities="report-status")
        assert refusal_report(request, self.REFUSED) == self.REPORT

    def test_report_status_v2(self):
        request = request_for((OLD_ID, NEW_ID, "refs/heads/main"), capabilities="report-status-v2")
        assert refusal_report(request, self.REFUSED) == self.REPORT

    def test_side_band_64k(self):
        request = request_for((OLD_ID, NEW_ID, "refs/heads/main"), capabilities="report-status side-band-64k")
        response = refusal_report(request, self.REFUSED)
        assert response == pkt(b"\1" + self.REPORT) + b"0000"

    def test_side_band_splits_at_1000_bytes(self):
        updates = [(OLD_ID, NEW_ID, f"refs/heads/branch-{i:03d}") for i in range(40)]
        request = request_for(*updates, capabilities="report-status side-band")
        refused = {update.ref: "the git grant does not cover this branch" for update in request.updates}
        response = refusal_report(request, refused)
        assert response is not None
        assert max(int(response[i : i + 4], 16) for i in range(0, len(response) - 4, 1000)) <= 1000
        assert unband(response).startswith(pkt(b"unpack ok\n"))
        assert unband(response).count(b"ng refs/heads/branch-") == 40

    def test_every_update_in_order(self):
        request = request_for((OLD_ID, NEW_ID, "refs/heads/seer/fix"), (OLD_ID, NEW_ID, "refs/heads/main"))
        response = refusal_report(request, refused_updates(RULES, request))
        assert response is not None
        assert unband(response) == (
            pkt(b"unpack ok\n")
            + pkt(b"ng refs/heads/seer/fix refused with the rest of the push\n")
            + pkt(b"ng refs/heads/main the git grant does not cover this branch\n")
            + b"0000"
        )

    def test_client_asked_for_no_report(self):
        request = request_for((OLD_ID, NEW_ID, "refs/heads/main"), capabilities="side-band-64k quiet")
        assert refusal_report(request, self.REFUSED) is None


class TestReplayedBody:
    @pytest.mark.parametrize("size", [1, 3, 5, 64, 1024])
    def test_head_then_rest(self, size):
        body = ReplayedBody(b"head", io.BytesIO(b"rest of the body"))
        chunks = []
        while chunk := body.read(size):
            chunks.append(chunk)
        assert b"".join(chunks) == b"headrest of the body"
        assert all(len(chunk) <= size for chunk in chunks)

    def test_read_all(self):
        assert ReplayedBody(b"head", io.BytesIO(b"rest")).read() == b"headrest"

    def test_empty_head(self):
        assert ReplayedBody(b"", io.BytesIO(b"rest")).read(64) == b"rest"


class Recording(io.BytesIO):
    """A body that tells how far it was read."""


class TestCheckPush:
    def check(self, body: bytes, rules=RULES, headers: dict[str, str] | None = None) -> tuple[bytes, Recording]:
        stream = Recording(body)
        relayed = check_push(rules, headers or {}, stream)
        return relayed.read(), stream

    def test_allowed_push_relays_the_whole_body(self):
        body = make_push_body((OLD_ID, NEW_ID, "refs/heads/seer/fix"))
        relayed, _ = self.check(body)
        assert relayed == body

    def test_probe(self):
        relayed, _ = self.check(b"0000")
        assert relayed == b"0000"

    def test_no_updates_with_more_after_them(self):
        # A provider reads nothing after a flush that updates no ref, so whatever follows has no business there.
        with pytest.raises(GitMalformedRequestBody, match="updates no ref but goes on"):
            self.check(b"0000" + PACK)

    def test_refused_push_is_read_to_the_end(self):
        stream = Recording(make_push_body((OLD_ID, NEW_ID, "refs/heads/main"), capabilities="report-status"))
        with pytest.raises(PushRefused) as exc_info:
            check_push(RULES, {}, stream)
        assert stream.read() == b""
        assert exc_info.value.code == "git_operation_not_granted"
        assert exc_info.value.detail == (
            "The git grant does not allow this push: refs/heads/main (the git grant does not cover this branch)."
        )
        assert exc_info.value.report == (
            pkt(b"unpack ok\n") + pkt(b"ng refs/heads/main the git grant does not cover this branch\n") + b"0000"
        )

    def test_no_rules_refuse_every_update(self):
        with pytest.raises(PushRefused, match="does not cover this branch"):
            self.check(make_push_body((OLD_ID, NEW_ID, "refs/heads/seer/fix")), rules=[])

    def test_no_body(self):
        with pytest.raises(GitMalformedRequestBody, match="has no body"):
            check_push(RULES, {}, None)

    @pytest.mark.parametrize(
        "body,headers,error",
        [
            (b"garbage and then some", {}, GitMalformedRequestBody),
            (
                make_push_body((OLD_ID, NEW_ID, "refs/heads/seer/fix")),
                {"Content-Encoding": "gzip"},
                RpcMalformedRequestHeaders,
            ),
            (pkt(b"push-cert\0report-status") + b"0000" + PACK, {}, PushRefused),
        ],
        ids=["malformed", "compressed", "signed"],
    )
    def test_other_refusals_are_read_to_the_end(self, body, headers, error):
        stream = Recording(body)
        with pytest.raises(error):
            check_push(RULES, headers, stream)
        assert stream.read() == b""

    def test_signed_push_on_a_side_band(self):
        # git prints band 3 as "remote: <message>" before it stops, where a 403 would show only its status.
        body = pkt(b"push-cert\0report-status side-band-64k") + pkt(b"certificate version 0.1\n") + b"0000"
        with pytest.raises(PushRefused) as exc_info:
            self.check(body)
        assert exc_info.value.report == pkt(b"\3The git endpoint does not relay signed pushes.\n") + b"0000"

    def test_signed_push_without_a_side_band(self):
        with pytest.raises(PushRefused) as exc_info:
            self.check(pkt(b"push-cert\0report-status") + b"0000")
        assert exc_info.value.report is None

    def test_body_that_ends_early(self):
        body = make_push_body((OLD_ID, NEW_ID, "refs/heads/seer/fix"))
        with pytest.raises(GitMalformedRequestBody, match="ended before its ref updates did"):
            self.check(body[:20])

    def test_long_ref_is_cut_short_in_the_detail(self):
        ref = "refs/notes/" + "x" * 1000
        with pytest.raises(PushRefused) as exc_info:
            self.check(make_push_body((OLD_ID, NEW_ID, ref), capabilities="report-status"))
        assert exc_info.value.detail == (
            f"The git grant does not allow this push: {ref[:200]}... "
            "(the git grant allows pushing branches and tags only)."
        )
        # The report names the whole ref, since git matches it to the ref it pushed.
        assert ref.encode() in (exc_info.value.report or b"")

    @pytest.mark.parametrize(
        "ref",
        ["HEAD", "seer/fix", "refs/heads/" + "x" * 256, "refs/tags/" + "x" * 256, "refs/tags/-v1", "refs/tags/HEAD"],
        ids=["head", "bare", "long-branch", "long-tag", "dash-tag", "head-tag"],
    )
    def test_ref_git_would_not_send(self, ref):
        # git sends refs under refs/, and a branch or tag name past the cap would make a rule's pattern slow.
        with pytest.raises(GitMalformedRequestBody):
            self.check(make_push_body((OLD_ID, NEW_ID, ref)))

    def test_longest_branch_name(self):
        ref = "refs/heads/seer/" + "x" * 250
        rules = [PushRule(repository=5, branch_patterns=["seer/x+"])]
        relayed, _ = self.check(make_push_body((OLD_ID, NEW_ID, ref)), rules=rules)
        assert ref.encode() in relayed
