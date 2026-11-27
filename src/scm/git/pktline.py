"""The pkt-line framing git's fetch and push share, gitprotocol-common(5), and the object formats their ids come in."""

import re

from scm.errors import SCMCodedError

PKT_LENGTH = re.compile(rb"[0-9a-f]{4}")
"""A pkt-line's length, in 4 lowercase hex digits as git writes them, counting the 4 digits too."""

MAX_PKT_LENGTH = 65520

FLUSH = 0
"""A flush, the pkt-line ``0000``, as ``split_pkt_lines`` returns it: by the length it gives."""

DELIM = 1
"""A delimiter, the pkt-line ``0001`` of protocol version 2, as ``split_pkt_lines`` returns it."""

OBJECT_ID_LENGTHS = {"sha1": 40, "sha256": 64}
"""The object formats git names in object-format=, and the length of an object id in each, in hex."""

_HEX = re.compile(r"[0-9a-f]+")


def pkt(payload: bytes) -> bytes:
    """Return ``payload`` as a pkt-line."""
    return b"%04x" % (len(payload) + 4) + payload


def split_pkt_lines(data: bytes, error: type[SCMCodedError], detail: str) -> list[bytes | int]:
    """Split ``data`` into the payloads of its pkt-lines, with a flush as ``FLUSH`` and a delimiter as ``DELIM``.

    Raises ``error`` with ``detail`` for data that isn't pkt-lines.
    """
    lines: list[bytes | int] = []
    index = 0
    while index < len(data):
        header = data[index : index + 4]
        if not PKT_LENGTH.fullmatch(header):
            raise error(detail=detail)
        length = int(header, 16)
        if length in (FLUSH, DELIM):
            lines.append(length)
            index += 4
            continue
        if not 4 < length <= MAX_PKT_LENGTH or index + length > len(data):
            raise error(detail=detail)
        lines.append(data[index + 4 : index + length])
        index += length
    return lines


def is_object_id(text: str, length: int) -> bool:
    """Whether ``text`` is an object id of ``length`` digits, in lowercase hex as git writes one."""
    return len(text) == length and _HEX.fullmatch(text) is not None
