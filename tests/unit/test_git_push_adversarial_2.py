"""A second round of pushes built to slip past the push check, against a3fbc3c. Each test fails while the check lets
its input through."""

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
    """_check_headers compares ``value.strip()`` with git's values, and the relay sends ``value`` on as it came.

    str.strip() removes more than the spaces and tabs an HTTP server trims: U+00A0 and U+0085, which reach a WSGI
    application as the header bytes 0xA0 and 0x85, and the controls 0x0B, 0x0C and 0x1C to 0x1F. So a value git never
    sends passes the check and reaches the provider.
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
        # It passes the check, then requests refuses to send a header value that starts with whitespace. That
        # surfaces as resource_bad_gateway, a 502 that says the provider failed and that a client retries, after the
        # repository was looked up and a token fetched for a request the check should have refused.
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
        # _NOT_IN_A_REF keeps ASCII control characters out of a ref, as git does, but git takes any byte from 0x80
        # up. So a ref that decodes to a C1 control, a Unicode line or paragraph separator (str.splitlines() splits
        # on U+0085, U+2028 and U+2029) or a bidirectional override passes as a name and lands in the refusal's
        # detail, which goes to logs and to a Sentry issue's title.
        body = pkt(f"{OLD_ID} {NEW_ID} {ref}".encode() + b"\0report-status") + b"0000"
        with pytest.raises((GitOperationNotGranted, GitMalformedRequestBody)) as exc_info:
            check_push(RULES, {}, io.BytesIO(body))
        assert exc_info.value.detail.isprintable(), ascii(exc_info.value.detail)

    def test_route_in_the_detail_is_printable_and_cut_short(self):
        # The route is the URL path below <repo>.git/, percent-decoded by the caller's web server, and
        # GitRouteNotSupported quotes it whole. Sentry's URL pattern takes any character but a newline there, so an
        # escape or a carriage return and as many bytes as a URL holds reach the same logs a ref's name is kept
        # short and clean for.
        recorder = Recorder()
        route = "git-receive-pack\x1b[2J\rERROR forged log line " + "x" * 5000
        response = request(make_server(recorder), PUSH_GRANT, method="POST", route=route, query="")
        assert response.status_code == 404
        detail = recorder.errors[0][0].detail  # type: ignore[attr-defined]
        assert detail.isprintable(), ascii(detail[:80])
        assert len(detail) <= 1000


class TestShallowClone:
    def test_push_from_a_clone_with_many_shallow_commits(self):
        # git lists every shallow commit of the clone ahead of the updates of every push it makes, 53 bytes each
        # with SHA-1. MAX_UPDATES_LENGTH counts them, so a push of one branch from a clone with more than about
        # 1,230 shallow commits is refused as malformed. `git clone --depth=1 --no-single-branch` of a repository
        # with 1,400 branches makes such a clone, and its push through GitServer fails with "RPC failed; HTTP 400".
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
        # check_push runs on the POST only. A push's first request, GET info/refs?service=git-receive-pack, is
        # authorized by the same push rule and relayed with whatever the client put in the headers the POST checks.
        # git asks for no protocol version there, or for version 1, and sends no Content-Encoding on a GET.
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
        # A refused push is read to its end so that git reads the answer. Nothing bounds that read: a client that
        # holds any push grant sends four bytes that are no pkt-line and then streams for as long as it likes, and
        # the worker reads along with no provider on the other side to cut it off. 8 GiB is past any provider's
        # push limit (GitHub's is 2 GiB), so nothing a provider would have taken needs reading that far. Sentry's
        # caller buffers the body before it calls GitServer, so this bites only a caller that streams.
        give_up_after = 8 * 1024**3
        body = Endless(b"zzzz", give_up_after)
        with pytest.raises(GitMalformedRequestBody):
            check_push(RULES, {}, body)
        assert body.supplied < give_up_after, "the refused push was read for 8 GiB and would have been read on"
