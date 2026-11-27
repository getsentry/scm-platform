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

``check_fetch`` reads a request with ``scm.git.fetch.request`` and checks it against ``scm.git.fetch.policy``.
``scm.git.fetch.upstream`` holds what GitServer then asks the provider and reads from it.
"""

import io
from collections.abc import Mapping, Sequence
from typing import NamedTuple

from scm.errors import GitMalformedRequestBody
from scm.git.fetch.headers import header_values, replacing_header, without_header
from scm.git.fetch.policy import INFINITE_DEPTH, check_arguments, check_capabilities
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
    filter_capabilities,
    read_refs,
    refs_headers,
    refs_request,
    refs_response,
)
from scm.git.pktline import OBJECT_ID_LENGTHS, pkt
from scm.git.types import FetchRule, Readable

__all__ = [
    "INFINITE_DEPTH",
    "MAX_ADVERTISEMENT_LENGTH",
    "MAX_REFS_LENGTH",
    "MAX_REQUEST_LENGTH",
    "Command",
    "FetchRefused",
    "FetchRequest",
    "Refs",
    "check_fetch",
    "check_wants",
    "filter_capabilities",
    "read_refs",
    "refs_headers",
    "refs_request",
    "refs_response",
]


class FetchRequest(NamedTuple):
    """A fetch's request, read and checked, and what to relay for it."""

    command: Command | None
    """The command a POST runs, or None for the GET that lists the server's capabilities."""
    headers: dict[str, str]
    """The headers to relay the GET or a ``fetch`` with. The GET's response, which the endpoint reads, is asked for
    without compression, and a ``fetch`` goes on uncompressed, with the length of its new body."""
    body: Readable | None
    """The body to relay a ``fetch`` with: the pkt-lines the client sent, without ``include-tag``. GitServer answers
    ``ls-refs`` itself, so it has none."""
    wants: frozenset[str] = frozenset()
    """The object ids a ``fetch`` command wants, which must be tips the rules cover."""
    object_format: str = "sha1"
    """The object format a POST names. Without one, git means SHA-1."""
    unborn: bool = False
    """Whether an ``ls-refs`` asks for ``HEAD`` when it names a branch with no commit yet, which git asks only of a
    provider that offers it."""


def check_fetch(
    rules: Sequence[FetchRule], method: str, headers: Mapping[str, str], body: Readable | None
) -> FetchRequest:
    """Read and check a fetch's request under ``rules``, none of which allows any object.

    ``body`` must end where the request body ends. Raises GitOperationNotGranted for a request that isn't protocol
    version 2, FetchRefused for a command or argument the endpoint refuses, GitMalformedRequestBody for a body that
    isn't a protocol version 2 request, RpcMalformedRequestHeaders for an encoding git doesn't send, and
    ClientClosedRequest if reading the body fails. The wants are checked later, against the tips, by
    ``check_wants``.
    """
    check_headers(method, headers)
    if method != "POST":
        return FetchRequest(command=None, headers=replacing_header(headers, "Accept-Encoding", "identity"), body=None)
    if body is None:
        raise GitMalformedRequestBody(detail="The fetch has no body.")
    raw = read_body(body)
    request = read_request(decompress(raw) if header_values(headers, "content-encoding") else raw)
    check_capabilities(request.capabilities)
    object_format = object_format_of(request.capabilities)
    if request.command == "ls-refs":
        # GitServer answers ls-refs from its own listing, so the client's other arguments don't matter.
        return FetchRequest(
            command=request.command,
            object_format=object_format,
            headers=dict(headers),
            body=None,
            unborn="unborn" in request.arguments,
        )
    wants = check_arguments(rules, request.arguments, OBJECT_ID_LENGTHS[object_format])
    # Under include-tag, upload-pack adds the annotated tags that point into the pack, the ones the rules hide too.
    # Without it, git fetch asks for the covered tags it follows by name, though a single-branch clone gets them only
    # on its first fetch. check_capabilities has refused include-tag as a capability, so any line left is an argument.
    relayed = b"".join(
        b"%04x" % line if isinstance(line, int) else pkt(line)
        for line in request.lines
        if line not in (b"include-tag", b"include-tag\n")
    )
    return FetchRequest(
        command=request.command,
        wants=wants,
        object_format=object_format,
        headers=replacing_header(without_header(headers, "content-encoding"), "Content-Length", str(len(relayed))),
        body=io.BytesIO(relayed),
    )


def check_wants(request: FetchRequest, tips: frozenset[str]) -> None:
    """Refuse a fetch that wants anything but ``tips``, the objects of the refs the rules cover. Raises FetchRefused."""
    refused = sorted(request.wants - tips)
    if refused:
        raise FetchRefused(
            detail=f"The git grant does not allow fetching {refused[0]}: it is not the tip of a branch or tag the "
            "grant covers, or that branch has moved since. Fetch a branch the grant covers by its name, and fetch "
            "again if it moved."
        )
