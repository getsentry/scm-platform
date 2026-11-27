"""Reads a fetch's request: its headers, its body, and the command, capabilities and arguments the body holds.

Reading refuses what isn't a protocol version 2 request for a command the endpoint relays. What a command may ask
for is ``scm.git.fetch.policy``'s to say.
"""

import re
import zlib
from collections.abc import Mapping
from typing import Literal, NamedTuple

from scm.errors import GitMalformedRequestBody, GitOperationNotGranted, RpcMalformedRequestHeaders
from scm.git.fetch.headers import header_values
from scm.git.pktline import DELIM, FLUSH, OBJECT_ID_LENGTHS, pkt, split_pkt_lines
from scm.git.relay import _printable, _read
from scm.git.types import Readable

MAX_REQUEST_LENGTH = 8 * 1024 * 1024
"""The most bytes a fetch's request may take, before and after decompressing it. A request lists the wants and a
round of haves, each about 50 bytes."""

type Command = Literal["ls-refs", "fetch"]

# What git puts in a request's lines: printable ASCII. A NUL or a control byte could make a line read differently
# here and in a provider that reads it as a C string.
_REQUEST_LINE = re.compile(rb"[\x20-\x7e]+")
# The most of a client-chosen name a refusal's detail shows. The detail reaches error reports and logs.
_SHOWN_LENGTH = 100


class FetchRefused(GitOperationNotGranted):
    """A fetch command the grant does not allow, with the response that makes git show why.

    git reads a protocol version 2 response's ``ERR`` line as the server's message and stops with it, where a
    refused HTTP request shows only its status code.
    """

    def __init__(self, *, detail: str) -> None:
        super().__init__(detail=detail)
        self.report = pkt(f"ERR {detail}\n".encode())


class CommandRequest(NamedTuple):
    """The command a POST runs, as the client sent it."""

    command: Command
    capabilities: list[str]
    """The lines between the command and the delimiter, without their newlines."""
    arguments: list[str]
    """The lines after the delimiter, without their newlines."""
    lines: list[bytes | int]
    """Every pkt-line of the request as ``split_pkt_lines`` returns them, to relay the request from."""


def check_headers(method: str, headers: Mapping[str, str]) -> None:
    """Refuse a request that isn't in protocol version 2, or whose body is encoded in a way git doesn't send.

    Raises GitOperationNotGranted and RpcMalformedRequestHeaders.
    """
    protocols = header_values(headers, "git-protocol")
    if not protocols or any(value != "version=2" for value in protocols):
        raise GitOperationNotGranted(
            detail="The git grant's fetch rules name branches and tags, which the git endpoint keeps a fetch to only "
            "in git protocol version 2. Fetch with git 2.26 or later, or set protocol.version=2."
        )
    # git compresses a large request with gzip and sends a small one as it is.
    if method == "POST" and any(value != "gzip" for value in header_values(headers, "content-encoding")):
        raise RpcMalformedRequestHeaders(detail="The fetch's content-encoding header holds a value git doesn't send.")


def read_body(body: Readable) -> bytes:
    """Return the whole of ``body``. Raises GitMalformedRequestBody for one longer than MAX_REQUEST_LENGTH."""
    data = bytearray()
    while len(data) <= MAX_REQUEST_LENGTH and (chunk := _read(body, MAX_REQUEST_LENGTH + 1 - len(data))):
        data += chunk
    if len(data) > MAX_REQUEST_LENGTH:
        raise GitMalformedRequestBody(detail=f"The fetch's request is longer than {MAX_REQUEST_LENGTH} bytes.")
    return bytes(data)


def decompress(raw: bytes) -> bytes:
    """Return a gzipped body decompressed. Raises GitMalformedRequestBody for anything but a single gzip stream, or
    for one longer than MAX_REQUEST_LENGTH once decompressed."""
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


def read_request(data: bytes) -> CommandRequest:
    """Return the command a request's body runs, with its capabilities and arguments.

    Raises GitMalformedRequestBody for a body that isn't a protocol version 2 request, and FetchRefused for a command
    other than ``ls-refs`` and ``fetch``.
    """
    lines = split_pkt_lines(data, GitMalformedRequestBody, "The fetch's request is not pkt-lines.")
    if not lines or lines[-1] != FLUSH or lines.count(FLUSH) > 1:
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
                detail=f"The git endpoint does not relay the {shown(other)} command under a "
                "grant whose fetch rules name branches and tags."
            )
    if texts.count(DELIM) > 1:
        raise GitMalformedRequestBody(detail="The fetch's request has more than one list of arguments.")
    # The arguments follow a delimiter, which a request without arguments leaves out.
    delim = texts.index(DELIM) if DELIM in texts else len(texts) - 1
    return CommandRequest(
        command=command,
        capabilities=[text for text in texts[1:delim] if isinstance(text, str)],
        arguments=[text for text in texts[delim + 1 : -1] if isinstance(text, str)],
        lines=lines,
    )


def object_format_of(capabilities: list[str]) -> str:
    """Return the object format a request names among its capabilities.

    Raises GitMalformedRequestBody for more than one format, or for one git doesn't have.
    """
    formats = [
        value
        for name, _, value in (capability.partition("=") for capability in capabilities)
        if name == "object-format"
    ]
    if len(formats) > 1:
        raise GitMalformedRequestBody(detail="The fetch names more than one object format.")
    # Without object-format=, git means SHA-1.
    object_format = formats[0] if formats else "sha1"
    if object_format not in OBJECT_ID_LENGTHS:
        raise GitMalformedRequestBody(detail="The fetch names an object format git doesn't have.")
    return object_format


def shown(text: str) -> str:
    """Return a client-chosen name as a refusal's detail shows it."""
    return _printable(text, _SHOWN_LENGTH)
