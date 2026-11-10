"""More pushes built to slip past the push check, each of which it must refuse."""

import io

import pytest

from scm.errors import GitMalformedRequestBody, GitOperationNotGranted, RpcMalformedRequestHeaders
from scm.git.push import check_push
from scm.git.relay import DEFAULT_CHUNK_SIZE, GitRelay
from scm.git.types import GrantV2PushRule
from tests.unit.git_builders import NEW_ID, OLD_ID, make_push_body, make_repository, pkt
from tests.unit.git_upstream import running_upstream
from tests.unit.test_git_server import PUSH_GRANT, Recorder, make_server, request

RULES = [GrantV2PushRule(repository=5, branches=["seer/fix"])]
ALLOWED_PUSH = make_push_body((OLD_ID, NEW_ID, "refs/heads/seer/fix"))
GIT_PROTOCOL_VALUES = (None, "version=0", "version=1")


def send_push(upstream, headers: dict[str, str]) -> tuple[Recorder, int]:
    recorder = Recorder(repository=make_repository(provider_name="github_enterprise", web_base_url=upstream.url))
    response = request(
        make_server(recorder, GitRelay(ca_bundle=upstream.ca_bundle)),
        PUSH_GRANT,
        method="POST",
        route="git-receive-pack",
        query="",
        headers=headers,
        body=io.BytesIO(ALLOWED_PUSH),
    )
    b"".join(response.content)
    return recorder, response.status_code


class TestHeaderValuesAsRelayed:
    """Header values are checked as the relay sends them on, not stripped.

    str.strip() removes more than the spaces and tabs an HTTP server trims: U+00A0 and U+0085, which reach a WSGI
    application as the header bytes 0xA0 and 0x85, and the controls 0x0B, 0x0C and 0x1C to 0x1F. So a check of
    stripped values would pass values git never sends.
    """

    @pytest.mark.parametrize(
        "name,value",
        [
            ("Git-Protocol", "version=1\xa0"),
            ("Git-Protocol", "version=1\x85"),
            ("Git-Protocol", "version=1\x0c"),
            ("Git-Protocol", "version=1\x1f"),
            ("Accept", "application/x-git-receive-pack-result\xa0"),
            ("Content-Type", "application/x-git-receive-pack-request\x85"),
            ("Content-Encoding", "identity\xa0"),
            ("Content-Encoding", "\x85"),
        ],
    )
    def test_value_padded_with_what_only_strip_removes_is_refused(self, name, value):
        with pytest.raises((RpcMalformedRequestHeaders, GitOperationNotGranted)):
            check_push(RULES, {name: value}, io.BytesIO(ALLOWED_PUSH))

    @pytest.mark.parametrize("value", ["version=1\xa0", "version=1\x85", "version=1\x0c"])
    def test_padded_git_protocol_never_reaches_the_provider(self, value):
        with running_upstream() as upstream:
            send_push(upstream, {"Git-Protocol": value})
        assert all(r.headers.get("Git-Protocol") in GIT_PROTOCOL_VALUES for r in upstream.requests)

    @pytest.mark.parametrize(
        "headers",
        [{"Git-Protocol": "\xa0version=1"}, {"Accept": "\x85application/x-git-receive-pack-result"}],
        ids=["git-protocol", "accept"],
    )
    def test_value_padded_in_front_is_refused_as_the_clients_fault(self, headers):
        # requests refuses to send a value that starts with whitespace, so letting it past the check would answer the
        # client's mistake with a 502 that blames the provider, after fetching a token.
        with running_upstream() as upstream:
            recorder, status = send_push(upstream, headers)
        assert recorder.error_codes == ["rpc_malformed_request_headers"]
        assert status == 400
        assert recorder.token_lookups == []


class TestRefusalDetailUnicode:
    @pytest.mark.parametrize(
        "ref",
        [
            "refs/tags/x\u2028ERROR-forged-log-line",
            "refs/tags/x\u2029y",
            "refs/tags/x\u0085ERROR-forged-log-line",
            "refs/tags/x\u009b2Jy",
            "refs/heads/main\u202egnp.exe",
            "refs/heads/seer/fix\u200b",
        ],
        ids=["line-separator", "paragraph-separator", "next-line", "c1-csi", "right-to-left-override", "zero-width"],
    )
    def test_detail_holds_only_printable_characters(self, ref):
        # git takes any byte from 0x80 up in a ref, so a name can decode to a line separator, a C1 control or a
        # bidirectional override, and the detail goes to logs and to a Sentry issue's title.
        body = pkt(f"{OLD_ID} {NEW_ID} {ref}".encode() + b"\0report-status") + b"0000"
        with pytest.raises((GitOperationNotGranted, GitMalformedRequestBody)) as exc_info:
            check_push(RULES, {}, io.BytesIO(body))
        assert exc_info.value.detail.isprintable(), ascii(exc_info.value.detail)

    def test_route_in_the_detail_is_printable_and_cut_short(self):
        # The caller's web server percent-decodes the route, so it can hold escapes, carriage returns and as many
        # bytes as a URL holds, and GitRouteNotSupported's detail reaches the same logs as a ref's name.
        recorder = Recorder()
        route = "git-receive-pack\x1b[2J\rERROR forged log line " + "x" * 5000
        response = request(make_server(recorder), PUSH_GRANT, method="POST", route=route, query="")
        assert response.status_code == 404
        detail = recorder.errors[0][0].detail  # type: ignore[attr-defined]
        assert detail.isprintable(), ascii(detail[:80])
        assert len(detail) <= 1000


class TestShallowClone:
    def test_push_from_a_clone_with_many_shallow_commits(self):
        # git lists every shallow commit ahead of a push's updates, so a depth-1 clone of 1,400 branches sends about
        # 74 KB of them, more than the updates' own budget.
        shallow = tuple(f"{index:040x}" for index in range(1, 1401))
        body = make_push_body((OLD_ID, NEW_ID, "refs/heads/seer/fix"), shallow=shallow)
        assert check_push(RULES, {}, io.BytesIO(body)).read() == body


class TestRefAdvertisementOfAPush:
    @pytest.mark.parametrize(
        "name,value,sent_by_git",
        [
            ("Git-Protocol", "version=2:future-key=value", GIT_PROTOCOL_VALUES),
            ("Content-Encoding", "gzip", (None,)),
        ],
        ids=["git-protocol", "content-encoding"],
    )
    def test_header_value_git_does_not_send_stays_behind(self, name, value, sent_by_git):
        # A push's first request, GET info/refs?service=git-receive-pack, goes to the provider with the same headers.
        # git asks for no protocol version there, or for version 1, and sends no Content-Encoding.
        with running_upstream() as upstream:
            recorder = Recorder(
                repository=make_repository(provider_name="github_enterprise", web_base_url=upstream.url)
            )
            response = request(
                make_server(recorder, GitRelay(ca_bundle=upstream.ca_bundle)),
                PUSH_GRANT,
                query="service=git-receive-pack",
                headers={name: value},
            )
            b"".join(response.content)
        assert all(r.headers.get(name) in sent_by_git for r in upstream.requests)


class Endless:
    """A push whose body never ends, as a chunked request body a caller streams can be."""

    CHUNK = bytes(DEFAULT_CHUNK_SIZE)

    def __init__(self, head: bytes, give_up_after: int) -> None:
        self.head = head
        self.give_up_after = give_up_after
        self.supplied = 0

    def read(self, size: int = -1, /) -> bytes:
        if self.head:
            data, self.head = self.head[:size], self.head[size:]
            return data
        if self.supplied >= self.give_up_after:
            return b""
        data = self.CHUNK[:size]
        self.supplied += len(data)
        return data


class TestDrain:
    def test_refused_push_is_not_read_without_bound(self):
        # A refused push is read to its end so that git reads the answer, and a client that never stops would hold
        # the worker. 8 GiB is past any provider's push limit, so nothing a provider would take needs reading that far.
        give_up_after = 8 * 1024**3
        body = Endless(b"zzzz", give_up_after)
        with pytest.raises(GitMalformedRequestBody):
            check_push(RULES, {}, body)
        assert body.supplied < give_up_after, "the refused push was read for 8 GiB and would have been read on"
