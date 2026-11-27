"""Limits a fetch to the branches and tags its grant's fetch rules name.

upload-pack sends any object a fetch names by id, whether a ref reaches it or not, and on GitHub also the objects of
the repository's forks. So under such rules GitServer:

- Relays only git protocol version 2, since versions 0 and 1 list every ref up front.
- Answers ``ls-refs`` itself with ``HEAD`` and the covered branches and tags (``refs_request``), and drops what it
  refuses from the provider's list of capabilities.
- Refuses a ``want`` that isn't the current tip of a covered branch or tag, or the object a covered tag points to.
- Refuses what reaches past the wants: a ``deepen`` without limit, and ``deepen-relative`` before git 2.54, under
  which upload-pack deepens commits the client names as shallow. Also ``want-ref``, ``packfile-uris``, server
  options, sparse and combined filters, and every command but ``ls-refs`` and ``fetch``.

``have``, ``shallow`` and ``thin-pack`` pass, since git sends them on ordinary fetches and the endpoint can't tell
which objects the covered refs reach. They do tell a client about an object it names by full id: upload-pack
acknowledges a ``have`` it holds and fails on a ``shallow`` of anything but a commit, and a thin pack may delta an
allowed object against one the client claims to have, which shows how alike the two are. Unlike a ref name, an id
can't be guessed without the whole content of the object, which it hashes. ``include-tag`` is dropped, because
upload-pack would add the annotated tags that point into the pack, the hidden ones too.

``check_fetch`` reads a request with ``scm.git.fetch.request`` and checks it against ``scm.git.fetch.policy``,
before GitServer looks anything up. ``respond`` then answers it from the provider, with what
``scm.git.fetch.upstream`` asks the provider and reads from it.
"""

import io
from collections.abc import Mapping, Sequence
from typing import NamedTuple

from scm.errors import GitMalformedRequestBody
from scm.git.fetch.headers import header_values, replacing_header, without_header
from scm.git.fetch.policy import INFINITE_DEPTH, check_arguments, check_capabilities, check_wants
from scm.git.fetch.request import (
    MAX_REQUEST_LENGTH,
    Command,
    FetchRefused,
    check_headers,
    decompress,
    object_format_of,
    read_body,
    read_request,
)
from scm.git.fetch.upstream import (
    MAX_ADVERTISEMENT_LENGTH,
    MAX_REFS_LENGTH,
    Refs,
    Upstream,
    filter_capabilities,
    list_refs,
    read_refs,
    refs_headers,
    refs_request,
    refs_response,
)
from scm.git.pktline import OBJECT_ID_LENGTHS, pkt
from scm.git.relay import RelayResponse
from scm.git.types import FetchRule, Readable

__all__ = [
    "INFINITE_DEPTH",
    "MAX_ADVERTISEMENT_LENGTH",
    "MAX_REFS_LENGTH",
    "MAX_REQUEST_LENGTH",
    "Advertisement",
    "Command",
    "Fetch",
    "FetchRefused",
    "FetchRequest",
    "LsRefs",
    "Refs",
    "Upstream",
    "check_fetch",
    "check_wants",
    "filter_capabilities",
    "list_refs",
    "read_refs",
    "refs_headers",
    "refs_request",
    "refs_response",
    "respond",
]


class Advertisement(NamedTuple):
    """The GET that lists the provider's capabilities."""

    headers: dict[str, str]
    """The headers to relay the GET with. Its response, which the endpoint reads, is asked for without compression."""


class LsRefs(NamedTuple):
    """An ``ls-refs`` command, which the endpoint answers from its own listing of the covered refs."""

    headers: dict[str, str]
    """The client's headers, for the user agent the listing names."""
    object_format: str
    """The object format the request names. Without one, git means SHA-1."""
    unborn: bool
    """Whether the client asks for ``HEAD`` when it names a branch with no commit yet, which git asks only of a
    provider that offers it."""


class Fetch(NamedTuple):
    """A ``fetch`` command that asks for nothing the endpoint refuses, with its wants still to check."""

    headers: dict[str, str]
    """The headers to relay the command with: uncompressed, with the length of its new body."""
    body: Readable
    """The body to relay: the pkt-lines the client sent, without ``include-tag``."""
    wants: frozenset[str]
    """The object ids the command wants, which must be tips the rules cover."""
    object_format: str
    """The object format the request names. Without one, git means SHA-1."""


type FetchRequest = Advertisement | LsRefs | Fetch
"""A fetch's request, read and checked: one of the three requests git makes in protocol version 2."""


def check_fetch(
    rules: Sequence[FetchRule], method: str, headers: Mapping[str, str], body: Readable | None
) -> FetchRequest:
    """Read and check a fetch's request under ``rules``, none of which allows any object.

    ``body`` must end where the request body ends. Raises GitOperationNotGranted for a request that isn't protocol
    version 2, FetchRefused for a command or argument the endpoint refuses, GitMalformedRequestBody for a body that
    isn't a protocol version 2 request, RpcMalformedRequestHeaders for an encoding git doesn't send, and
    ClientClosedRequest if reading the body fails. The wants are checked later, against the tips, by ``respond``.
    """
    check_headers(method, headers)
    if method != "POST":
        return Advertisement(headers=replacing_header(headers, "Accept-Encoding", "identity"))
    if body is None:
        raise GitMalformedRequestBody(detail="The fetch has no body.")
    raw = read_body(body)
    request = read_request(decompress(raw) if header_values(headers, "content-encoding") else raw)
    check_capabilities(request.capabilities)
    object_format = object_format_of(request.capabilities)
    if request.command == "ls-refs":
        # The endpoint answers ls-refs from its own listing, so the client's other arguments don't matter.
        return LsRefs(headers=dict(headers), object_format=object_format, unborn="unborn" in request.arguments)
    wants = check_arguments(rules, request.arguments, OBJECT_ID_LENGTHS[object_format])
    # Under include-tag, upload-pack adds the annotated tags that point into the pack, the ones the rules hide too.
    # Without it, git fetch asks for the covered tags it follows by name, though a single-branch clone gets them only
    # on its first fetch. check_capabilities has refused include-tag as a capability, so any line left is an argument.
    relayed = b"".join(
        b"%04x" % line if isinstance(line, int) else pkt(line)
        for line in request.lines
        if line not in (b"include-tag", b"include-tag\n")
    )
    return Fetch(
        headers=replacing_header(without_header(headers, "content-encoding"), "Content-Length", str(len(relayed))),
        body=io.BytesIO(relayed),
        wants=wants,
        object_format=object_format,
    )


def respond(rules: Sequence[FetchRule], request: FetchRequest, upstream: Upstream) -> RelayResponse:
    """Answer a request that ``check_fetch`` read under ``rules``, from the provider.

    Raises FetchRefused for a ``fetch`` that wants anything but a current tip of the refs ``rules`` cover, and what
    ``list_refs`` and ``filter_capabilities`` raise for a provider's answer the endpoint can't use.
    """
    match request:
        case Advertisement():
            return filter_capabilities(
                upstream.send(
                    method="GET", route="info/refs", query="service=git-upload-pack", headers=request.headers, body=None
                )
            )
        case LsRefs():
            return refs_response(
                list_refs(upstream, rules, request.headers, request.object_format, unborn=request.unborn)
            )
        case Fetch():
            # upload-pack sends nothing for a command without wants, so there are no tips to list for it.
            if request.wants:
                check_wants(request.wants, list_refs(upstream, rules, request.headers, request.object_format).tips)
            return upstream.send(
                method="POST", route="git-upload-pack", query="", headers=request.headers, body=request.body
            )
