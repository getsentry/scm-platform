"""The pkt-line framing git's fetch and push share, gitprotocol-common(5), and the object formats their ids come in."""

import re

PKT_LENGTH = re.compile(rb"[0-9a-f]{4}")
"""A pkt-line's length, in 4 lowercase hex digits as git writes them, counting the 4 digits too."""

MAX_PKT_LENGTH = 65520

OBJECT_ID_LENGTHS = {"sha1": 40, "sha256": 64}
"""The object formats git names in object-format=, and the length of an object id in each, in hex."""


def pkt(payload: bytes) -> bytes:
    """Return ``payload`` as a pkt-line."""
    return b"%04x" % (len(payload) + 4) + payload
