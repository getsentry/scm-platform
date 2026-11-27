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
    Advertisement,
    Fetch,
    FetchRefused,
    LsRefs,
    check_fetch,
    check_wants,
    filter_capabilities,
    read_refs,
    refs_headers,
    refs_request,
    refs_response,
    respond,
)
from scm.git.relay import RelayResponse
from scm.git.types import FetchRule, Readable
from tests.unit.git_builders import NEW_ID, OLD_ID, SERVICE_HEADER, make_fetch_body, pkt, pkt_lines

RULES = [FetchRule(repository=5, branches=["seer/fix", "main"])]
V2 = {"Git-Protocol": "version=2"}
POST_HEADERS = V2 | {"Content-Type": "application/x-git-upload-pack-request"}
TIP = "a" * 40
OTHER = "b" * 40
CLONE = ("thin-pack", "no-progress", "ofs-delta", "deepen 4", f"want {TIP}", "done")


def fetch(*arguments: str, **kwargs) -> Fetch:
    request = check_fetch(RULES, "POST", POST_HEADERS, io.BytesIO(make_fetch_body("fetch", *arguments, **kwargs)))
    assert isinstance(request, Fetch)
    return request


def chunks(*parts: bytes) -> Generator[bytes]:
    yield from parts


def response(*parts: bytes, status_code: int = 200, headers: dict[str, str] | None = None) -> RelayResponse:
    return RelayResponse(status_code=status_code, headers=headers or {}, content=chunks(*parts))


class TestHeaders:
    def test_capabilities_are_asked_for_unencoded(self):
        request = check_fetch(RULES, "GET", V2 | {"accept-encoding": "deflate, gzip"}, None)
        assert request == Advertisement(headers=V2 | {"Accept-Encoding": "identity"})

    @pytest.mark.parametrize(
        "headers",
        [{}, {"Git-Protocol": "version=1"}, {"Git-Protocol": "version=2", "git-protocol": "version=0"}],
        ids=["version-0", "version-1", "second-spelling"],
    )
    @pytest.mark.parametrize("method", ["GET", "POST"])
    def test_only_protocol_version_2(self, method, headers):
        # Versions 0 and 1 list every ref in their first response.
        with pytest.raises(GitOperationNotGranted, match="only in git protocol version 2"):
            check_fetch(RULES, method, headers, io.BytesIO(make_fetch_body("ls-refs")))

    def test_encoding_git_doesnt_send(self):
        with pytest.raises(RpcMalformedRequestHeaders, match="content-encoding"):
            check_fetch(RULES, "POST", POST_HEADERS | {"Content-Encoding": "deflate"}, io.BytesIO(b""))


class TestLsRefs:
    def test_arguments_dont_matter(self):
        # GitServer answers ls-refs from its own listing.
        body = make_fetch_body("ls-refs", "symrefs", "ref-prefix refs/heads/secret", "exclude refs/heads/x")
        request = check_fetch(RULES, "POST", POST_HEADERS, io.BytesIO(body))
        assert request == LsRefs(headers=POST_HEADERS, object_format="sha1", unborn=False)

    def test_unborn(self):
        # git asks for unborn only of a provider that offers it, so the endpoint asks the provider only then too.
        body = make_fetch_body("ls-refs", "symrefs", "unborn")
        request = check_fetch(RULES, "POST", POST_HEADERS, io.BytesIO(body))
        assert request == LsRefs(headers=POST_HEADERS, object_format="sha1", unborn=True)

    def test_object_format(self):
        body = make_fetch_body("ls-refs", capabilities=("object-format=sha256",))
        request = check_fetch(RULES, "POST", POST_HEADERS, io.BytesIO(body))
        assert request == LsRefs(headers=POST_HEADERS, object_format="sha256", unborn=False)


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
        assert fetch(*arguments).wants == {TIP}

    def test_include_tag_is_dropped(self):
        # upload-pack would add the annotated tags the rules hide that point into the pack.
        request = fetch(f"want {TIP}", "include-tag", "done")
        relayed = make_fetch_body("fetch", f"want {TIP}", "done")
        assert request.body.read() == relayed
        assert request.headers["Content-Length"] == str(len(relayed))

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
            # upload-pack fails a deepen-not that names no ref, which tells whether a hidden ref exists.
            ("deepen-not refs/heads/secret", "deepen-not of refs/heads/secret"),
            ("deepen-not main", "deepen-not of main, which is not the full name"),
            ("upload-pack-option x", "does not relay a fetch with upload-pack-option"),
        ],
    )
    def test_refused(self, argument, detail):
        # A forged shallow commit under a depth without limit or deepen-relative gets its history, whatever the wants.
        with pytest.raises(FetchRefused, match=detail) as exc_info:
            fetch(f"want {TIP}", f"shallow {OTHER}", argument, "done")
        assert exc_info.value.report == pkt(f"ERR {exc_info.value.detail}\n".encode())

    @pytest.mark.parametrize("command", ["object-info", "bundle-uri", "unknown"])
    def test_other_commands(self, command):
        with pytest.raises(FetchRefused, match=f"the {command} command"):
            check_fetch(RULES, "POST", POST_HEADERS, io.BytesIO(make_fetch_body(command, f"oid {TIP}")))

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
            "want " + TIP[:-1],
            "want " + TIP.upper(),
            "have " + "c" * 64,
            "shallow HEAD",
        ],
    )
    def test_malformed_argument(self, argument):
        with pytest.raises(GitMalformedRequestBody):
            fetch(f"want {TIP}", argument)


@pytest.mark.parametrize("command", ["ls-refs", "fetch"])
class TestCapabilities:
    @pytest.mark.parametrize(
        "capability,detail",
        [
            ("server-option=x", "does not relay server options"),
            ("trace2=1", "the trace2 capability"),
            # Dropped from a fetch's arguments, so it must not pass as a capability either.
            ("include-tag", "the include-tag capability"),
        ],
    )
    def test_refused(self, command, capability, detail):
        body = make_fetch_body(command, capabilities=("agent=git/2.54.0", capability))
        with pytest.raises(FetchRefused, match=detail):
            check_fetch(RULES, "POST", POST_HEADERS, io.BytesIO(body))

    @pytest.mark.parametrize(
        "capabilities,detail",
        [
            (("object-format=sha1", "object-format=sha256"), "more than one object format"),
            (("object-format=md5",), "an object format git doesn't have"),
        ],
    )
    def test_object_format_git_doesnt_send(self, command, capabilities, detail):
        with pytest.raises(GitMalformedRequestBody, match=detail):
            check_fetch(RULES, "POST", POST_HEADERS, io.BytesIO(make_fetch_body(command, capabilities=capabilities)))


class TestFirstFailure:
    """A refused request gets a report git shows and a malformed one a 400, so the order of the checks shows."""

    def test_command_before_the_lists_of_arguments(self):
        body = pkt(b"command=object-info\n") + b"0001" + pkt(b"size\n") + b"0001" + pkt(b"size\n") + b"0000"
        with pytest.raises(FetchRefused, match="the object-info command"):
            check_fetch(RULES, "POST", POST_HEADERS, io.BytesIO(body))

    def test_every_capability_before_the_object_format(self):
        with pytest.raises(FetchRefused, match="the trace2 capability"):
            fetch(capabilities=("object-format=sha1", "object-format=sha256", "trace2=1"))

    @pytest.mark.parametrize(
        "capability,error,detail",
        [
            ("trace2=1", FetchRefused, "the trace2 capability"),
            ("object-format=md5", GitMalformedRequestBody, "an object format git doesn't have"),
        ],
    )
    @pytest.mark.parametrize("argument", ["deepen-relative", "want nonsense"])
    def test_capabilities_before_arguments(self, capability, error, detail, argument):
        with pytest.raises(error, match=detail):
            fetch(argument, capabilities=(capability,))

    @pytest.mark.parametrize(
        "arguments,error",
        [
            (("deepen-relative", "want nonsense"), FetchRefused),
            (("want nonsense", "deepen-relative"), GitMalformedRequestBody),
        ],
    )
    def test_arguments_in_the_order_sent(self, arguments, error):
        with pytest.raises(error):
            fetch(*arguments)


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
            check_fetch(RULES, "POST", POST_HEADERS, io.BytesIO(body))

    def test_object_format_mismatch(self):
        with pytest.raises(GitMalformedRequestBody, match="does not name an object id"):
            fetch(f"want {TIP}", capabilities=("object-format=sha256",))

    def test_no_body(self):
        with pytest.raises(GitMalformedRequestBody, match="no body"):
            check_fetch(RULES, "POST", POST_HEADERS, None)

    def test_too_long(self):
        body = make_fetch_body("fetch", *[f"have {OLD_ID}"] * (MAX_REQUEST_LENGTH // 40))
        with pytest.raises(GitMalformedRequestBody, match="longer than"):
            check_fetch(RULES, "POST", POST_HEADERS, io.BytesIO(body))


class TestCompressed:
    headers = POST_HEADERS | {"Content-Encoding": "gzip"}

    def test_read_and_relayed_decompressed(self):
        body = make_fetch_body("fetch", *CLONE)
        request = check_fetch(RULES, "POST", self.headers, io.BytesIO(gzip.compress(body)))
        assert isinstance(request, Fetch)
        assert request.wants == {TIP}
        assert request.body.read() == body
        assert request.headers == POST_HEADERS | {"Content-Length": str(len(body))}

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
            check_fetch(RULES, "POST", self.headers, io.BytesIO(body))

    def test_too_long_once_decompressed(self):
        body = gzip.compress(make_fetch_body("fetch", *[f"have {OLD_ID}"] * (MAX_REQUEST_LENGTH // 40)))
        with pytest.raises(GitMalformedRequestBody, match="longer than"):
            check_fetch(RULES, "POST", self.headers, io.BytesIO(body))


class TestCheckWants:
    def test_tips(self):
        check_wants(fetch(*CLONE).wants, frozenset({TIP, OTHER}))

    def test_want_that_is_not_a_tip(self):
        with pytest.raises(
            FetchRefused, match=f"does not allow fetching {OTHER}: it is not the tip.*fetch again"
        ) as exc_info:
            check_wants(fetch(f"want {TIP}", f"want {OTHER}").wants, frozenset({TIP}))
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

    def test_unborn(self):
        assert refs_request(RULES, "sha1", unborn=True) == make_fetch_body(
            "ls-refs",
            "symrefs",
            "peel",
            "unborn",
            "ref-prefix HEAD",
            "ref-prefix refs/heads/main",
            "ref-prefix refs/heads/seer/fix",
            capabilities=("object-format=sha1",),
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


class FakeUpstream:
    """A provider that answers each request with the next of ``responses``, keeping what it was sent."""

    def __init__(self, *responses: RelayResponse) -> None:
        self.responses = list(responses)
        self.sent: list[tuple[str, str, str, dict[str, str], bytes | None]] = []

    def send(self, *, method: str, route: str, query: str, headers, body: Readable | None) -> RelayResponse:
        self.sent.append((method, route, query, dict(headers), None if body is None else body.read()))
        return self.responses.pop(0)


class TestRespond:
    listing = pkt_lines(f"{TIP} HEAD symref-target:refs/heads/main", f"{TIP} refs/heads/main", f"{OTHER} refs/heads/x")
    listed = (
        "POST",
        "git-upload-pack",
        "",
        refs_headers({"User-Agent": "git/2.54.0"}),
        refs_request(RULES, "sha1"),
    )
    headers = POST_HEADERS | {"User-Agent": "git/2.54.0"}

    def test_advertisement_loses_what_the_endpoint_refuses(self):
        upstream = FakeUpstream(response(advertisement(CAPABILITIES)))
        answer = respond(RULES, check_fetch(RULES, "GET", V2, None), upstream)
        filtered = filter_capabilities(response(advertisement(CAPABILITIES)))
        content = b"".join(answer.content)
        assert content == b"".join(filtered.content)
        assert b"bundle-uri" not in content
        assert upstream.sent == [
            ("GET", "info/refs", "service=git-upload-pack", V2 | {"Accept-Encoding": "identity"}, None)
        ]

    @pytest.mark.parametrize("unborn", [False, True])
    def test_ls_refs_is_answered_from_the_endpoints_own_listing(self, unborn):
        upstream = FakeUpstream(response(self.listing))
        body = make_fetch_body("ls-refs", "ref-prefix refs/heads/x", *(["unborn"] if unborn else []))
        answer = respond(RULES, check_fetch(RULES, "POST", self.headers, io.BytesIO(body)), upstream)
        assert b"".join(answer.content) == pkt_lines(
            f"{TIP} HEAD symref-target:refs/heads/main", f"{TIP} refs/heads/main"
        )
        assert upstream.sent == [(*self.listed[:4], refs_request(RULES, "sha1", unborn=unborn))]

    def test_fetch_of_a_tip_goes_on_once_the_tips_are_listed(self):
        pack = response(b"PACK")
        upstream = FakeUpstream(response(self.listing), pack)
        body = make_fetch_body("fetch", *CLONE)
        request = check_fetch(RULES, "POST", self.headers, io.BytesIO(body))
        assert respond(RULES, request, upstream) is pack
        assert upstream.sent == [
            self.listed,
            ("POST", "git-upload-pack", "", self.headers | {"Content-Length": str(len(body))}, body),
        ]

    def test_fetch_of_anything_else_never_reaches_the_provider(self):
        listing = chunks(self.listing)
        upstream = FakeUpstream(RelayResponse(status_code=200, headers={}, content=listing))
        body = make_fetch_body("fetch", f"want {TIP}", f"want {OTHER}", "done")
        with pytest.raises(FetchRefused, match=f"does not allow fetching {OTHER}"):
            respond(RULES, check_fetch(RULES, "POST", self.headers, io.BytesIO(body)), upstream)
        assert upstream.sent == [self.listed]
        assert listing.gi_frame is None

    def test_fetch_without_wants_lists_no_tips(self):
        pack = response(b"0000")
        upstream = FakeUpstream(pack)
        body = make_fetch_body("fetch", f"have {TIP}", "done")
        assert respond(RULES, check_fetch(RULES, "POST", self.headers, io.BytesIO(body)), upstream) is pack
        assert [sent[4] for sent in upstream.sent] == [body]

    def test_provider_that_fails_to_list_the_refs(self):
        failed = chunks(b"Internal Server Error")
        upstream = FakeUpstream(RelayResponse(status_code=500, headers={}, content=failed))
        with pytest.raises(ResourceBadGateway, match="answered HTTP 500 when the git endpoint listed"):
            respond(RULES, fetch(*CLONE), upstream)
        assert failed.gi_frame is None
