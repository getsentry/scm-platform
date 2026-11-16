"""Keeping a fetch to its grant's fetch rules when none of them allows any object.

git fetches over protocol version 2, gitprotocol-v2(5): a GET that lists the server's capabilities, then a POST for
each command. ``ls-refs`` lists refs, and ``fetch`` asks for a pack of the objects the client wants, named by their
ids. upload-pack sends any object it holds that a fetch names, whether a ref reaches it or not, and on GitHub that
includes the objects of the repository's forks. So GitServer keeps such a fetch to the rules itself:

- Only protocol version 2 is relayed, since versions 0 and 1 list every ref in their first response.
- The capabilities the provider lists leave out what the endpoint refuses, so git doesn't ask for it.
- GitServer answers ``ls-refs`` itself, whatever the client asked for, with ``HEAD`` and the branches and tags the
  rules cover, which it lists from the provider with ``refs_request``. git keeps to the refs it asked for itself.
- Each ``want`` must be the current tip of a covered branch or tag, or the object a covered tag points to.
  ``check_wants`` checks them against the same listing.
- A fetch may not reach past its wants. Under a ``deepen`` without limit (``git fetch --unshallow``) upload-pack
  deepens every commit the client names as shallow, and under ``deepen-relative`` (``git fetch --deepen``) every one
  a ref reaches, until git 2.54, which providers may not run yet. So a client could name a commit of another branch
  as shallow and get its history. Both are refused, as are ``want-ref``, ``packfile-uris``, server options,
  filters that read a sparse-checkout file from the repository, and every command other than ``ls-refs`` and
  ``fetch``, such as ``object-info`` and ``bundle-uri``.

``have`` and ``shallow`` lines pass on unchecked. A ``have`` only leaves objects out of the pack, though with
``thin-pack`` the provider may send an object as a delta against one the client claims to have, which git's own docs
list as revealing how the two are alike. Under a deepen with a limit, upload-pack deepens a shallow commit only if it
lies within that depth of the wants. ``include-tag`` lets the provider add annotated tags that point into the pack,
whatever their names.
"""

import io
import re
import zlib
from collections.abc import Generator, Mapping, Sequence
from typing import Literal, NamedTuple

from scm.errors import (
    GitMalformedRequestBody,
    GitOperationNotGranted,
    GitProviderNotSupported,
    ResourceBadGateway,
    RpcMalformedRequestHeaders,
    SCMCodedError,
)
from scm.git.push import _MAX_PKT_LENGTH, _OBJECT_ID_LENGTHS, _PKT_LENGTH, _pkt
from scm.git.relay import RelayResponse, ResponseBody, _printable, _read
from scm.git.types import FetchRule, Readable

MAX_REQUEST_LENGTH = 8 * 1024 * 1024
"""The most bytes a fetch's request may take, before and after decompressing it. A request lists the wants and a
round of haves, each about 50 bytes."""

MAX_ADVERTISEMENT_LENGTH = 64 * 1024
"""The most bytes of the provider's list of capabilities the endpoint reads. Providers send under 1 KiB."""

MAX_REFS_LENGTH = 16 * 1024 * 1024
"""The most bytes of the provider's list of covered refs the endpoint reads. A rule with a branch pattern lists every
branch, about 100 bytes each."""

INFINITE_DEPTH = 2**31 - 1
"""The depth at which upload-pack unshallows every commit the client names as shallow, and the depth any larger one
wraps to when upload-pack reads it into an int."""

type Command = Literal["ls-refs", "fetch"]

_FLUSH = 0
_DELIM = 1
# What git puts in a request's lines: printable ASCII. A NUL or a control byte could make a line read differently
# here and in a provider that reads it as a C string.
_REQUEST_LINE = re.compile(rb"[\x20-\x7e]+")
_HEX = re.compile(r"[0-9a-f]+")
# git writes a depth with %d and a time with PRItime. upload-pack reads both with base 0, so a leading 0 would make
# it read octal and 0x hex.
_DEPTH = re.compile(r"[1-9][0-9]{0,9}")
_TIME = re.compile(r"0|[1-9][0-9]{0,19}")
_NAME = re.compile(r"[\x21-\x7e]+")
# Filters that only leave objects out. sparse:oid reads a file from the repository, and combine: hides the filters
# it combines, so both are refused.
_FILTER = re.compile(r"blob:none|blob:limit=[0-9]+[kmg]?|tree:[0-9]+|object:type=(?:blob|tree|commit|tag)")
_FETCH_FLAGS = frozenset(
    {"done", "include-tag", "no-progress", "ofs-delta", "sideband-all", "thin-pack", "wait-for-done"}
)
# Capabilities a request may carry before its arguments. git sends server options only with -o, and they ask the
# provider to act on the fetch.
_REQUEST_CAPABILITIES = frozenset({"agent", "object-format", "session-id"})
# What the endpoint takes out of the provider's capabilities, as commands and as features of the fetch command.
_REFUSED_COMMANDS = frozenset({b"bundle-uri", b"object-info"})
_REFUSED_FETCH_FEATURES = frozenset({b"packfile-uris", b"ref-in-want"})
# The most of a client-chosen name a refusal's detail shows. The detail reaches error reports and logs.
_SHOWN_LENGTH = 100


class FetchRefused(GitOperationNotGranted):
    """A fetch command the grant does not allow, with the response that makes git show why.

    git reads a protocol version 2 response's ``ERR`` line as the server's message and stops with it, where a
    refused HTTP request shows only its status code.
    """

    def __init__(self, *, detail: str) -> None:
        super().__init__(detail=detail)
        self.report = _pkt(f"ERR {detail}\n".encode())


class FetchRequest(NamedTuple):
    """A fetch's request, read and checked, and what to relay for it."""

    command: Command | None
    """The command a POST runs, or None for the GET that lists the server's capabilities."""
    headers: dict[str, str]
    """The headers to relay the GET or a ``fetch`` with. The GET's response, which the endpoint reads, is asked for
    without compression, and a ``fetch`` goes on with the length of the body the endpoint read."""
    body: Readable | None
    """The body to relay a ``fetch`` with: the bytes the client sent, as it sent them. GitServer answers ``ls-refs``
    itself, so it has none."""
    wants: frozenset[str] = frozenset()
    """The object ids a ``fetch`` command wants, which must be tips the rules cover."""
    object_format: str = "sha1"
    """The object format a POST names. Without one, git means SHA-1."""


def check_fetch(method: str, headers: Mapping[str, str], body: Readable | None) -> FetchRequest:
    """Read and check a fetch's request under fetch rules that don't allow any object.

    ``body`` must end where the request body ends. Raises GitOperationNotGranted for a request that isn't protocol
    version 2, FetchRefused for a command or argument the endpoint refuses, GitMalformedRequestBody for a body that
    isn't a protocol version 2 request, RpcMalformedRequestHeaders for an encoding git doesn't send, and
    ClientClosedRequest if reading the body fails. The wants are checked later, against the tips, by
    ``check_wants``.
    """
    _check_headers(method, headers)
    if method != "POST":
        return FetchRequest(command=None, headers=_replacing(headers, "Accept-Encoding", "identity"), body=None)
    if body is None:
        raise GitMalformedRequestBody(detail="The fetch has no body.")
    raw = _read_body(body)
    data = _decompress(raw) if _header_values(headers, "content-encoding") else raw
    command, object_format, arguments = _read_request(data)
    if command == "ls-refs":
        # GitServer answers ls-refs from its own listing, so the client's arguments don't matter.
        return FetchRequest(command=command, object_format=object_format, headers=dict(headers), body=None)
    return FetchRequest(
        command=command,
        wants=_check_fetch_arguments(arguments, _OBJECT_ID_LENGTHS[object_format]),
        object_format=object_format,
        headers=_replacing(headers, "Content-Length", str(len(raw))),
        body=io.BytesIO(raw),
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


class Refs(NamedTuple):
    """The refs a listing found that the rules cover."""

    lines: list[bytes]
    """The provider's line for each covered branch and tag, and for ``HEAD`` when it points to one of them."""
    tips: frozenset[str]
    """The object ids of the covered branches and tags, and of the objects their tags point to."""


def refs_request(rules: Sequence[FetchRule], object_format: str) -> bytes:
    """Return the ``ls-refs`` request that lists ``HEAD`` and every branch and tag ``rules`` cover, and more.

    A prefix lists every ref whose name starts with it, so ``read_refs`` keeps only the refs a rule covers.
    """
    prefixes = {"HEAD"}
    for rule in rules:
        prefixes.update(rule.refs)
        if rule.branch_patterns:
            prefixes.add("refs/heads/")
        if rule.tag_patterns:
            prefixes.add("refs/tags/")
    arguments = ["symrefs", "peel", *(f"ref-prefix {prefix}" for prefix in sorted(prefixes))]
    return (
        _pkt(b"command=ls-refs\n")
        + _pkt(f"object-format={object_format}\n".encode())
        + b"0001"
        + b"".join(_pkt(f"{argument}\n".encode()) for argument in arguments)
        + b"0000"
    )


def refs_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Return the headers of the ``refs_request``, naming the client git's user agent as the client's request does."""
    agents = _header_values(headers, "user-agent")
    return {
        "Git-Protocol": "version=2",
        "Content-Type": "application/x-git-upload-pack-request",
        "Accept": "application/x-git-upload-pack-result",
        "Accept-Encoding": "identity",
    } | ({"User-Agent": agents[-1]} if agents else {})


def read_refs(rules: Sequence[FetchRule], response: RelayResponse, object_format: str) -> Refs:
    """Return the refs ``rules`` cover from the provider's answer to ``refs_request``.

    Raises ResourceBadGateway for an answer that isn't a list of refs.
    """
    _check_unencoded(response)
    data = _read_response(response.content, MAX_REFS_LENGTH, "list of branches and tags")
    id_length = _OBJECT_ID_LENGTHS[object_format]
    lines: list[bytes] = []
    tips: set[str] = set()
    for line in _response_lines(data):
        fields = _ref_fields(line)
        # A name that isn't UTF-8 can't be one a rule names.
        if fields is None:
            continue
        ref, object_id, attributes = fields
        if ref == "HEAD":
            target = next((value for key, value in attributes if key == "symref-target"), None)
            if target is None or not _covers(rules, target):
                continue
            # An empty repository's HEAD names a branch with no commit yet.
            ids = [] if object_id == "unborn" else [object_id]
        elif _covers(rules, ref):
            ids = [object_id, *(value for key, value in attributes if key == "peeled")]
            tips.update(ids)
        else:
            continue
        if any(len(tip) != id_length or not _HEX.fullmatch(tip) for tip in ids):
            raise ResourceBadGateway(detail="The provider listed a ref whose tip is not an object id.")
        lines.append(line)
    return Refs(lines=lines, tips=frozenset(tips))


def refs_response(refs: Refs) -> RelayResponse:
    """Return the ``ls-refs`` answer that lists ``refs``."""
    content = b"".join(_pkt(line) for line in refs.lines) + b"0000"
    return RelayResponse(
        status_code=200,
        headers={
            "Content-Type": "application/x-git-upload-pack-result",
            "Content-Length": str(len(content)),
            "Cache-Control": "no-cache",
        },
        content=_in_memory(content),
    )


def filter_capabilities(response: RelayResponse) -> RelayResponse:
    """Return the provider's list of capabilities without the commands and features the endpoint refuses.

    A response that isn't a success passes on, as it holds the provider's message. Raises GitProviderNotSupported for
    a provider that doesn't answer in protocol version 2, and ResourceBadGateway for a response the endpoint can't
    read.
    """
    if not 200 <= response.status_code < 300:
        return response
    try:
        _check_unencoded(response)
        content = _capabilities(_read_response(response.content, MAX_ADVERTISEMENT_LENGTH, "list of capabilities"))
    except Exception:
        response.content.close()
        raise
    return RelayResponse(
        status_code=response.status_code,
        headers=_replacing(response.headers, "Content-Length", str(len(content))),
        content=_in_memory(content),
    )


def _check_headers(method: str, headers: Mapping[str, str]) -> None:
    protocols = _header_values(headers, "git-protocol")
    if not protocols or any(value != "version=2" for value in protocols):
        raise GitOperationNotGranted(
            detail="The git grant's fetch rules name branches and tags, which the git endpoint keeps a fetch to only "
            "in git protocol version 2. Fetch with git 2.26 or later, or set protocol.version=2."
        )
    # git compresses a large request with gzip and sends a small one as it is.
    if method == "POST" and any(value != "gzip" for value in _header_values(headers, "content-encoding")):
        raise RpcMalformedRequestHeaders(detail="The fetch's content-encoding header holds a value git doesn't send.")


def _read_body(body: Readable) -> bytes:
    data = bytearray()
    while len(data) <= MAX_REQUEST_LENGTH and (chunk := _read(body, MAX_REQUEST_LENGTH + 1 - len(data))):
        data += chunk
    if len(data) > MAX_REQUEST_LENGTH:
        raise GitMalformedRequestBody(detail=f"The fetch's request is longer than {MAX_REQUEST_LENGTH} bytes.")
    return bytes(data)


def _decompress(raw: bytes) -> bytes:
    decompressor = zlib.decompressobj(wbits=16 + zlib.MAX_WBITS)
    try:
        data = decompressor.decompress(raw, MAX_REQUEST_LENGTH + 1)
    except zlib.error:
        raise GitMalformedRequestBody(detail="The fetch's request is not valid gzip.") from None
    if len(data) > MAX_REQUEST_LENGTH:
        raise GitMalformedRequestBody(detail=f"The fetch's request is longer than {MAX_REQUEST_LENGTH} bytes.")
    # A provider may read a second gzip member, or bytes after the first, as more of the request.
    if not decompressor.eof or decompressor.unused_data:
        raise GitMalformedRequestBody(detail="The fetch's request is not a single gzip stream.")
    return data


def _read_request(data: bytes) -> tuple[Command, str, list[str]]:
    """Return the command a request runs, its object format and its arguments."""
    lines = _pkt_lines(data, GitMalformedRequestBody, "The fetch's request is not pkt-lines.")
    if not lines or lines[-1] != _FLUSH or lines.count(_FLUSH) > 1:
        raise GitMalformedRequestBody(detail="The fetch's request does not end with its only flush.")
    texts: list[str | int] = []
    for line in lines:
        if isinstance(line, int):
            texts.append(line)
        elif _REQUEST_LINE.fullmatch(text := line.removesuffix(b"\n")):
            texts.append(text.decode("ascii"))
        else:
            raise GitMalformedRequestBody(detail="The fetch's request holds characters git doesn't send.")
    first = texts[0]
    if not isinstance(first, str) or not first.startswith("command="):
        raise GitMalformedRequestBody(detail="The fetch's request does not start with a command.")
    command: Command
    match first.removeprefix("command="):
        case "ls-refs":
            command = "ls-refs"
        case "fetch":
            command = "fetch"
        case other:
            raise FetchRefused(
                detail=f"The git endpoint does not relay the {_shown(other)} command under a "
                "grant whose fetch rules name branches and tags."
            )
    if texts.count(_DELIM) > 1:
        raise GitMalformedRequestBody(detail="The fetch's request has more than one list of arguments.")
    # The arguments follow a delimiter, which a request without arguments leaves out.
    delim = texts.index(_DELIM) if _DELIM in texts else len(texts) - 1
    capabilities = [text for text in texts[1:delim] if isinstance(text, str)]
    arguments = [text for text in texts[delim + 1 : -1] if isinstance(text, str)]
    return command, _object_format(capabilities), arguments


def _object_format(capabilities: list[str]) -> str:
    formats: list[str] = []
    for capability in capabilities:
        name, has_value, value = capability.partition("=")
        if name == "server-option":
            raise FetchRefused(detail="The git endpoint does not relay server options.")
        if name not in _REQUEST_CAPABILITIES or not has_value:
            raise FetchRefused(detail=f"The git endpoint does not relay the {_shown(name)} capability.")
        if name == "object-format":
            formats.append(value)
    if len(formats) > 1:
        raise GitMalformedRequestBody(detail="The fetch names more than one object format.")
    # Without object-format=, git means SHA-1.
    object_format = formats[0] if formats else "sha1"
    if object_format not in _OBJECT_ID_LENGTHS:
        raise GitMalformedRequestBody(detail="The fetch names an object format git doesn't have.")
    return object_format


def _check_fetch_arguments(arguments: list[str], id_length: int) -> frozenset[str]:
    """Refuse the arguments that could reach past the wants, and return the wants."""
    wants: set[str] = set()
    for argument in arguments:
        if argument in _FETCH_FLAGS:
            continue
        name, _, value = argument.partition(" ")
        if name in ("want", "have", "shallow"):
            if len(value) != id_length or not _HEX.fullmatch(value):
                raise GitMalformedRequestBody(detail=f"The fetch's {name} line does not name an object id.")
            if name == "want":
                wants.add(value)
        elif name == "deepen":
            if not _DEPTH.fullmatch(value):
                raise GitMalformedRequestBody(detail="The fetch's deepen line does not hold a depth git sends.")
            if int(value) >= INFINITE_DEPTH:
                raise FetchRefused(
                    detail="The git endpoint does not relay a fetch that deepens history without limit, such as git "
                    "fetch --unshallow, under a grant whose fetch rules name branches and tags. Fetch with "
                    "--depth=<n> instead."
                )
        elif argument == "deepen-relative":
            raise FetchRefused(
                detail="The git endpoint does not relay a fetch that deepens from the client's shallow commits, such "
                "as git fetch --deepen, under a grant whose fetch rules name branches and tags. Fetch with "
                "--depth=<n> instead."
            )
        elif name in ("deepen-since", "deepen-not"):
            if not (_TIME if name == "deepen-since" else _NAME).fullmatch(value):
                raise GitMalformedRequestBody(detail=f"The fetch's {name} line does not hold a value git sends.")
        elif name == "filter":
            if not _FILTER.fullmatch(value):
                raise FetchRefused(
                    detail=f"The git endpoint does not relay the filter {_shown(value)} under a "
                    "grant whose fetch rules name branches and tags."
                )
        elif name in ("want-ref", "packfile-uris"):
            raise FetchRefused(
                detail=f"The git endpoint does not relay {name} under a grant whose fetch rules name branches and tags."
            )
        else:
            raise FetchRefused(detail=f"The git endpoint does not relay a fetch with {_shown(name)}.")
    return frozenset(wants)


def _capabilities(data: bytes) -> bytes:
    """Return the provider's protocol version 2 list of capabilities without the ones the endpoint refuses."""
    lines = _pkt_lines(data, ResourceBadGateway, "The provider's list of capabilities is not pkt-lines.")
    # The smart-HTTP header that names the service, which a server may leave out in version 2.
    if lines[:2] == [b"# service=git-upload-pack\n", _FLUSH]:
        lines = lines[2:]
    if not lines or lines[0] not in (b"version 2\n", b"version 2") or lines[-1] != _FLUSH or _FLUSH in lines[:-1]:
        raise GitProviderNotSupported(
            detail="The provider did not answer in git protocol version 2, which the git endpoint needs to keep a "
            "fetch to the git grant's branches and tags."
        )
    kept: list[bytes] = [lines[0]]
    for line in lines[1:-1]:
        if not isinstance(line, bytes):
            raise ResourceBadGateway(detail="The provider's list of capabilities holds a delimiter.")
        name, has_value, value = line.removesuffix(b"\n").partition(b"=")
        if name in _REFUSED_COMMANDS:
            continue
        if name == b"fetch" and has_value:
            features = [feature for feature in value.split(b" ") if feature not in _REFUSED_FETCH_FEATURES]
            line = b"fetch=" + b" ".join(features) + b"\n"
        kept.append(line)
    return b"".join(_pkt(line) for line in kept) + b"0000"


def _in_memory(content: bytes) -> Generator[bytes]:
    yield content


def _ref_fields(line: bytes) -> tuple[str, str, list[tuple[str, str]]] | None:
    """Return a ref line's name, object id (or ``unborn``) and attributes, or None for a name that isn't UTF-8.

    Raises ResourceBadGateway for a line that isn't a ref.
    """
    try:
        text = line.removesuffix(b"\n").decode()
    except UnicodeDecodeError:
        return None
    fields = text.split(" ")
    attributes = [(key, value) for key, _, value in (field.partition(":") for field in fields[2:])]
    if len(fields) < 2 or not all(fields[:2]) or not all(key and value for key, value in attributes):
        raise ResourceBadGateway(detail="The provider's list of refs holds a line that is not a ref.")
    return fields[1], fields[0], attributes


def _covers(rules: Sequence[FetchRule], ref: str) -> bool:
    return any(rule.covers(ref) for rule in rules)


def _check_unencoded(response: RelayResponse) -> None:
    if any(value != "identity" for value in _header_values(response.headers, "content-encoding")):
        raise ResourceBadGateway(detail="The provider compressed a response the git endpoint has to read.")


def _read_response(content: ResponseBody, limit: int, what: str) -> bytes:
    data = bytearray()
    for chunk in content:
        data += chunk
        if len(data) > limit:
            content.close()
            raise ResourceBadGateway(detail=f"The provider's {what} is longer than {limit} bytes.")
    return bytes(data)


def _response_lines(data: bytes) -> list[bytes]:
    lines = _pkt_lines(data, ResourceBadGateway, "The provider's list of refs is not pkt-lines.")
    if not lines or lines[-1] != _FLUSH or _FLUSH in lines[:-1] or _DELIM in lines:
        raise ResourceBadGateway(detail="The provider's list of refs does not end with its only flush.")
    texts = [line for line in lines if isinstance(line, bytes)]
    error = next((line for line in texts if line.startswith(b"ERR ")), None)
    if error is not None:
        message = _printable(error.removeprefix(b"ERR ").decode(errors="replace").strip(), 200)
        raise ResourceBadGateway(detail=f"The provider refused to list the repository's refs: {message}")
    return texts


def _pkt_lines(data: bytes, error: type[SCMCodedError], detail: str) -> list[bytes | int]:
    """Split ``data`` into pkt-lines, with a flush as 0 and a delimiter as 1."""
    lines: list[bytes | int] = []
    index = 0
    while index < len(data):
        header = data[index : index + 4]
        if not _PKT_LENGTH.fullmatch(header):
            raise error(detail=detail)
        length = int(header, 16)
        if length in (_FLUSH, _DELIM):
            lines.append(length)
            index += 4
            continue
        if not 4 < length <= _MAX_PKT_LENGTH or index + length > len(data):
            raise error(detail=detail)
        lines.append(data[index + 4 : index + length])
        index += length
    return lines


def _shown(text: str) -> str:
    return _printable(text, _SHOWN_LENGTH)


def _header_values(headers: Mapping[str, str], name: str) -> list[str]:
    """Return the values of every spelling of ``name``, a lowercase header name."""
    return [value for key, value in headers.items() if key.lower() == name]


def _without(headers: Mapping[str, str], name: str) -> dict[str, str]:
    """Return ``headers`` without any spelling of ``name``, a lowercase header name."""
    return {key: value for key, value in headers.items() if key.lower() != name}


def _replacing(headers: Mapping[str, str], name: str, value: str) -> dict[str, str]:
    """Return ``headers`` with ``value`` as the only ``name`` header, whatever spelling it had."""
    return _without(headers, name.lower()) | {name: value}
