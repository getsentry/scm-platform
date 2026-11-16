import gzip
import io
from collections.abc import Generator

import pytest

from scm.errors import (
    GitMalformedRequestBody,
    GitOperationNotGranted,
    GitProviderNotSupported,
    ResourceBadGateway,
    RpcMalformedRequestHeaders,
)
from scm.git.fetch import (
    MAX_REQUEST_LENGTH,
    FetchRefused,
    FetchRequest,
    check_fetch,
    check_wants,
    filter_capabilities,
    read_refs,
    refs_headers,
    refs_request,
    refs_response,
)
from scm.git.relay import RelayResponse
from scm.git.types import FetchRule
from tests.unit.git_builders import NEW_ID, OLD_ID, SERVICE_HEADER, make_fetch_body, pkt, pkt_lines

RULES = [FetchRule(repository=5, branches=["seer/fix", "main"])]
V2 = {"Git-Protocol": "version=2"}
POST_HEADERS = V2 | {"Content-Type": "application/x-git-upload-pack-request"}
TIP = "a" * 40
OTHER = "b" * 40
CLONE = ("thin-pack", "no-progress", "ofs-delta", "deepen 4", f"want {TIP}", "done")


def fetch(*arguments: str, **kwargs) -> FetchRequest:
    return check_fetch("POST", POST_HEADERS, io.BytesIO(make_fetch_body("fetch", *arguments, **kwargs)))


def chunks(*parts: bytes) -> Generator[bytes]:
    yield from parts


def response(*parts: bytes, status_code: int = 200, headers: dict[str, str] | None = None) -> RelayResponse:
    return RelayResponse(status_code=status_code, headers=headers or {}, content=chunks(*parts))


class TestHeaders:
    def test_capabilities_are_asked_for_unencoded(self):
        request = check_fetch("GET", V2 | {"accept-encoding": "deflate, gzip"}, None)
        assert request.command is None
        assert request.headers == V2 | {"Accept-Encoding": "identity"}

    @pytest.mark.parametrize(
        "headers",
        [{}, {"Git-Protocol": "version=1"}, {"Git-Protocol": "version=2", "git-protocol": "version=0"}],
        ids=["version-0", "version-1", "second-spelling"],
    )
    @pytest.mark.parametrize("method", ["GET", "POST"])
    def test_only_protocol_version_2(self, method, headers):
        # Versions 0 and 1 list every ref in their first response.
        with pytest.raises(GitOperationNotGranted, match="only in git protocol version 2"):
            check_fetch(method, headers, io.BytesIO(make_fetch_body("ls-refs")))

    def test_encoding_git_doesnt_send(self):
        with pytest.raises(RpcMalformedRequestHeaders, match="content-encoding"):
            check_fetch("POST", POST_HEADERS | {"Content-Encoding": "deflate"}, io.BytesIO(b""))


class TestLsRefs:
    def test_arguments_dont_matter(self):
        # GitServer answers ls-refs from its own listing.
        body = make_fetch_body("ls-refs", "symrefs", "ref-prefix refs/heads/secret", "exclude refs/heads/x")
        request = check_fetch("POST", POST_HEADERS, io.BytesIO(body))
        assert request.command == "ls-refs"
        assert request.wants == frozenset()
        assert request.body is None

    def test_object_format(self):
        body = make_fetch_body("ls-refs", capabilities=("object-format=sha256",))
        assert check_fetch("POST", POST_HEADERS, io.BytesIO(body)).object_format == "sha256"


class TestFetchArguments:
    @pytest.mark.parametrize(
        "arguments",
        [
            CLONE,
            ("thin-pack", "ofs-delta", f"shallow {OLD_ID}", "deepen 10", f"want {TIP}", f"have {NEW_ID}", "done"),
            (f"want {TIP}", f"shallow {OLD_ID}", "deepen-since 1700000000", "include-tag"),
            (f"want {TIP}", "deepen-not refs/heads/main", "wait-for-done", "sideband-all"),
            (f"want {TIP}", "filter blob:none"),
            (f"want {TIP}", "filter blob:limit=1k"),
            (f"want {TIP}", "filter tree:0"),
            (f"want {TIP}", "filter object:type=commit"),
            (f"want {TIP}", "deepen 2147483646"),
        ],
        ids=[
            "clone",
            "fetch-with-depth",
            "deepen-since",
            "deepen-not",
            "blob-none",
            "blob-limit",
            "tree",
            "object-type",
            "largest-depth",
        ],
    )
    def test_allowed(self, arguments):
        request = fetch(*arguments)
        assert request.command == "fetch"
        assert request.wants == {TIP}

    def test_wants_are_collected(self):
        assert fetch(f"want {TIP}", f"want {OTHER}", f"want {TIP}", "done").wants == {TIP, OTHER}

    def test_sha256(self):
        tip = "c" * 64
        assert fetch(f"want {tip}", capabilities=("object-format=sha256",)).wants == {tip}

    @pytest.mark.parametrize(
        "argument,detail",
        [
            ("deepen 2147483647", "deepens history without limit, such as git fetch --unshallow"),
            # upload-pack reads the depth into an int, so this wraps to the depth without limit.
            ("deepen 6442450943", "deepens history without limit"),
            ("deepen-relative", "deepens from the client's shallow commits, such as git fetch --deepen"),
            ("want-ref refs/heads/main", "does not relay want-ref"),
            ("packfile-uris https", "does not relay packfile-uris"),
            ("filter sparse:oid=HEAD:sparse", "the filter sparse:oid=HEAD:sparse"),
            ("filter combine:blob:none+tree:0", "the filter combine:"),
            ("upload-pack-option x", "does not relay a fetch with upload-pack-option"),
        ],
    )
    def test_refused(self, argument, detail):
        # A forged shallow commit under a depth without limit or deepen-relative gets its history, whatever the wants.
        with pytest.raises(FetchRefused, match=detail) as exc_info:
            fetch(f"want {TIP}", f"shallow {OTHER}", argument, "done")
        assert exc_info.value.report == pkt(f"ERR {exc_info.value.detail}\n".encode())

    @pytest.mark.parametrize(
        "capability,detail",
        [("server-option=x", "does not relay server options"), ("trace2=1", "the trace2 capability")],
    )
    def test_refused_capability(self, capability, detail):
        with pytest.raises(FetchRefused, match=detail):
            fetch(*CLONE, capabilities=("agent=git/2.54.0", capability))

    @pytest.mark.parametrize("command", ["object-info", "bundle-uri", "unknown"])
    def test_other_commands(self, command):
        with pytest.raises(FetchRefused, match=f"the {command} command"):
            check_fetch("POST", POST_HEADERS, io.BytesIO(make_fetch_body(command, f"oid {TIP}")))

    @pytest.mark.parametrize(
        "argument",
        [
            # upload-pack reads a depth with base 0, so these would be octal and hex.
            "deepen 0x7fffffff",
            "deepen 017777777777",
            "deepen 0",
            "deepen -1",
            "deepen ",
            "deepen-since 0x10",
            "deepen-not",
            "want " + TIP[:-1],
            "want " + TIP.upper(),
            "have " + "c" * 64,
            "shallow HEAD",
        ],
    )
    def test_malformed_argument(self, argument):
        with pytest.raises(GitMalformedRequestBody):
            fetch(f"want {TIP}", argument)


class TestMalformed:
    @pytest.mark.parametrize(
        "body,detail",
        [
            (b"garbage", "not pkt-lines"),
            (make_fetch_body("fetch", f"want {TIP}")[:-4], "does not end with its only flush"),
            (make_fetch_body("fetch", f"want {TIP}") + pkt(b"deepen-relative\n"), "does not end with its only flush"),
            (make_fetch_body("fetch", f"want {TIP}") + b"0000", "does not end with its only flush"),
            (make_fetch_body("fetch", f"want {TIP}")[:-4] + b"0001" + pkt(b"done\n") + b"0000", "more than one list"),
            (pkt(b"want " + TIP.encode() + b"\n") + b"0000", "does not start with a command"),
            (
                pkt(b"command=fetch\n") + b"0001" + pkt(b"want " + TIP.encode() + b"\0deepen-relative\n") + b"0000",
                "git",
            ),
            (pkt(b"command=fetch\n") + b"0001" + pkt(b"\n") + b"0000", "characters git doesn't send"),
            (pkt(b"command=fetch\n") + b"0002" + b"0000", "not pkt-lines"),
        ],
        ids=[
            "not-pkt-lines",
            "no-flush",
            "after-flush",
            "two-flushes",
            "two-delimiters",
            "no-command",
            "nul",
            "empty-line",
            "response-end",
        ],
    )
    def test_body(self, body, detail):
        with pytest.raises(GitMalformedRequestBody, match=detail):
            check_fetch("POST", POST_HEADERS, io.BytesIO(body))

    def test_object_format_mismatch(self):
        with pytest.raises(GitMalformedRequestBody, match="does not name an object id"):
            fetch(f"want {TIP}", capabilities=("object-format=sha256",))

    def test_no_body(self):
        with pytest.raises(GitMalformedRequestBody, match="no body"):
            check_fetch("POST", POST_HEADERS, None)

    def test_too_long(self):
        body = make_fetch_body("fetch", *[f"have {OLD_ID}"] * (MAX_REQUEST_LENGTH // 40))
        with pytest.raises(GitMalformedRequestBody, match="longer than"):
            check_fetch("POST", POST_HEADERS, io.BytesIO(body))


class TestCompressed:
    headers = POST_HEADERS | {"Content-Encoding": "gzip"}

    def test_read_and_relayed_as_sent(self):
        body = gzip.compress(make_fetch_body("fetch", *CLONE))
        request = check_fetch("POST", self.headers, io.BytesIO(body))
        assert request.wants == {TIP}
        assert request.body is not None and request.body.read() == body
        assert request.headers["Content-Length"] == str(len(body))

    @pytest.mark.parametrize(
        "body",
        [
            # A provider that reads every member would see a fetch the endpoint didn't check.
            gzip.compress(make_fetch_body("fetch", *CLONE)) + gzip.compress(b"0001" + pkt(b"deepen-relative\n")),
            gzip.compress(make_fetch_body("fetch", *CLONE)) + b"trailing",
            gzip.compress(make_fetch_body("fetch", *CLONE))[:-8],
            b"not gzip",
        ],
        ids=["second-member", "trailing-bytes", "truncated", "not-gzip"],
    )
    def test_not_a_single_gzip_stream(self, body):
        with pytest.raises(GitMalformedRequestBody):
            check_fetch("POST", self.headers, io.BytesIO(body))

    def test_too_long_once_decompressed(self):
        body = gzip.compress(make_fetch_body("fetch", *[f"have {OLD_ID}"] * (MAX_REQUEST_LENGTH // 40)))
        with pytest.raises(GitMalformedRequestBody, match="longer than"):
            check_fetch("POST", self.headers, io.BytesIO(body))


class TestCheckWants:
    def test_tips(self):
        check_wants(fetch(*CLONE), frozenset({TIP, OTHER}))

    def test_want_that_is_not_a_tip(self):
        with pytest.raises(
            FetchRefused, match=f"does not allow fetching {OTHER}: it is not the tip.*fetch again"
        ) as exc_info:
            check_wants(fetch(f"want {TIP}", f"want {OTHER}"), frozenset({TIP}))
        assert exc_info.value.code == "git_operation_not_granted"


class TestRefsRequest:
    def test_listed_refs(self):
        rules = [FetchRule(repository=5, branches=["seer/fix", "main"]), FetchRule(repository=5, tags=["v1"])]
        assert refs_request(rules, "sha1") == make_fetch_body(
            "ls-refs",
            "symrefs",
            "peel",
            "ref-prefix HEAD",
            "ref-prefix refs/heads/main",
            "ref-prefix refs/heads/seer/fix",
            "ref-prefix refs/tags/v1",
            capabilities=("object-format=sha1",),
        )

    def test_patterns(self):
        rules = [FetchRule(repository=5, branch_patterns=["seer/.+"], tag_patterns=["v.+"])]
        assert refs_request(rules, "sha256") == make_fetch_body(
            "ls-refs",
            "symrefs",
            "peel",
            "ref-prefix HEAD",
            "ref-prefix refs/heads/",
            "ref-prefix refs/tags/",
            capabilities=("object-format=sha256",),
        )

    def test_headers(self):
        assert refs_headers({"user-agent": "git/2.54.0", "Content-Length": "9"}) == {
            "Git-Protocol": "version=2",
            "Content-Type": "application/x-git-upload-pack-request",
            "Accept": "application/x-git-upload-pack-result",
            "Accept-Encoding": "identity",
            "User-Agent": "git/2.54.0",
        }


class TestReadRefs:
    def test_covered_refs(self):
        rules = [*RULES, FetchRule(repository=5, tags=["v1"])]
        answer = pkt_lines(
            f"{TIP} HEAD symref-target:refs/heads/main",
            f"{TIP} refs/heads/main",
            # A prefix also lists names that start with it.
            f"{OTHER} refs/heads/main-old",
            f"{NEW_ID} refs/tags/v1 peeled:{OLD_ID}",
        )
        refs = read_refs(rules, response(answer), "sha1")
        assert refs.lines == [
            f"{TIP} HEAD symref-target:refs/heads/main\n".encode(),
            f"{TIP} refs/heads/main\n".encode(),
            f"{NEW_ID} refs/tags/v1 peeled:{OLD_ID}\n".encode(),
        ]
        assert refs.tips == {TIP, NEW_ID, OLD_ID}

    @pytest.mark.parametrize(
        "head,kept",
        [
            (f"{TIP} HEAD symref-target:refs/heads/main", True),
            (f"{TIP} HEAD symref-target:refs/heads/secret", False),
            # Without symrefs, nothing says which branch HEAD is.
            (f"{TIP} HEAD", False),
            ("unborn HEAD symref-target:refs/heads/main", True),
        ],
    )
    def test_head(self, head, kept):
        refs = read_refs(RULES, response(pkt_lines(head)), "sha1")
        assert refs.lines == ([f"{head}\n".encode()] if kept else [])
        # HEAD's tip is its branch's, which the branch's own line lists.
        assert refs.tips == frozenset()

    def test_name_that_is_not_utf8(self):
        answer = pkt(TIP.encode() + b" refs/heads/\xff\n") + b"0000"
        assert read_refs(RULES, response(answer), "sha1").lines == []

    @pytest.mark.parametrize(
        "answer,detail",
        [
            (pkt(b"ERR repository is disabled\n") + b"0000", "refused to list the repository's refs: repository is"),
            (pkt_lines(f"{TIP} refs/heads/main")[:-4], "does not end with its only flush"),
            (pkt_lines(f"{TIP} refs/heads/main") + pkt_lines(), "does not end with its only flush"),
            (pkt_lines("nonsense refs/heads/main"), "not an object id"),
            (pkt_lines("nonsense HEAD symref-target:refs/heads/main"), "not an object id"),
            (pkt_lines(f"{TIP} refs/heads/main peeled:nonsense"), "not an object id"),
            (pkt_lines("refs/heads/main"), "not a ref"),
            (b"garbage", "not pkt-lines"),
        ],
    )
    def test_answer_that_is_not_a_list_of_refs(self, answer, detail):
        with pytest.raises(ResourceBadGateway, match=detail):
            read_refs(RULES, response(answer), "sha1")

    def test_compressed_answer(self):
        with pytest.raises(ResourceBadGateway, match="compressed"):
            read_refs(RULES, response(b"", headers={"Content-Encoding": "gzip"}), "sha1")


def test_refs_response():
    lines = [f"{TIP} refs/heads/main\n".encode()]
    answer = refs_response(read_refs(RULES, response(pkt_lines(f"{TIP} refs/heads/main")), "sha1"))
    content = b"".join(answer.content)
    assert content == b"".join(pkt(line) for line in lines) + b"0000"
    assert answer.status_code == 200
    assert answer.headers["Content-Type"] == "application/x-git-upload-pack-result"
    assert answer.headers["Content-Length"] == str(len(content))


CAPABILITIES = [
    "version 2",
    "agent=git/github-f351b52fe3d7-Linux",
    "ls-refs=unborn",
    "fetch=shallow wait-for-done filter ref-in-want packfile-uris",
    "server-option",
    "object-format=sha1",
    "object-info",
    "bundle-uri",
]


def advertisement(lines: list[str], service: bool = True) -> bytes:
    return (SERVICE_HEADER if service else b"") + pkt_lines(*lines)


class TestFilterCapabilities:
    @pytest.mark.parametrize("service", [True, False], ids=["with-service-line", "without"])
    def test_refused_commands_and_features_are_taken_out(self, service):
        filtered = filter_capabilities(response(advertisement(CAPABILITIES, service)))
        content = b"".join(filtered.content)
        assert content == advertisement(
            [
                "version 2",
                "agent=git/github-f351b52fe3d7-Linux",
                "ls-refs=unborn",
                "fetch=shallow wait-for-done filter",
                "server-option",
                "object-format=sha1",
            ],
            service=False,
        )
        assert filtered.headers["Content-Length"] == str(len(content))

    def test_feature_that_is_not_utf8_passes_on(self):
        answer = pkt(b"version 2\n") + pkt(b"fetch=shallow \xff\n") + b"0000"
        assert b"".join(filter_capabilities(response(answer)).content) == answer

    def test_delimiter(self):
        answer = pkt(b"version 2\n") + b"0001" + pkt(b"ls-refs\n") + b"0000"
        with pytest.raises(ResourceBadGateway, match="holds a delimiter"):
            filter_capabilities(response(answer))

    def test_version_0_answer(self):
        # A provider that ignores Git-Protocol lists every ref.
        answer = advertisement([f"{TIP} HEAD\0multi_ack thin-pack", f"{OTHER} refs/heads/secret"])
        with pytest.raises(GitProviderNotSupported, match="did not answer in git protocol version 2"):
            filter_capabilities(response(answer, headers={"Content-Length": "1"}))

    def test_compressed_answer(self):
        content = chunks(b"")
        compressed = RelayResponse(status_code=200, headers={"Content-Encoding": "gzip"}, content=content)
        with pytest.raises(ResourceBadGateway, match="compressed"):
            filter_capabilities(compressed)
        assert content.gi_frame is None

    def test_provider_failure_passes_on(self):
        failed = response(b"Repository not found.", status_code=404)
        assert filter_capabilities(failed) is failed
