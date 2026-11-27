"""What a fetch kept to branches and tags may ask for, and what the endpoint takes out of the provider's offer.

``scm.git.fetch`` says why each is refused.
"""

import re
from collections.abc import Sequence

from scm.errors import GitMalformedRequestBody
from scm.git.fetch.request import FetchRefused, shown
from scm.git.pktline import is_object_id
from scm.git.types import FetchRule

INFINITE_DEPTH = 2**31 - 1
"""The depth at which upload-pack unshallows every commit the client names as shallow, and the depth any larger one
wraps to when upload-pack reads it into an int."""

REFUSED_COMMANDS = frozenset({b"bundle-uri", b"object-info"})
"""The commands the endpoint takes out of the provider's list of capabilities."""

REFUSED_FETCH_FEATURES = frozenset({b"packfile-uris", b"ref-in-want"})
"""The features of the fetch command the endpoint takes out of the provider's list of capabilities."""

# git writes a depth with %d and a time with PRItime. upload-pack reads both with base 0, so a leading 0 would make
# it read octal and 0x hex.
_DEPTH = re.compile(r"[1-9][0-9]{0,9}")
_TIME = re.compile(r"0|[1-9][0-9]{0,19}")
# Filters that only leave objects out. sparse:oid reads a file from the repository, and combine: hides the filters
# it combines, so both are refused.
_FILTER = re.compile(r"blob:none|blob:limit=[0-9]+[kmg]?|tree:[0-9]+|object:type=(?:blob|tree|commit|tag)")
_FETCH_FLAGS = frozenset(
    {"done", "include-tag", "no-progress", "ofs-delta", "sideband-all", "thin-pack", "wait-for-done"}
)
# Capabilities a request may carry before its arguments. git sends server options only with -o, and they ask the
# provider to act on the fetch.
_REQUEST_CAPABILITIES = frozenset({"agent", "object-format", "session-id"})


def covers(rules: Sequence[FetchRule], ref: str) -> bool:
    """Whether one of ``rules`` covers ``ref``, a branch's or tag's full name."""
    return any(rule.covers(ref) for rule in rules)


def check_capabilities(capabilities: list[str]) -> None:
    """Refuse a request that carries a capability other than the ones git sends with every command.

    Raises FetchRefused.
    """
    for capability in capabilities:
        name, has_value, _ = capability.partition("=")
        if name == "server-option":
            raise FetchRefused(detail="The git endpoint does not relay server options.")
        if name not in _REQUEST_CAPABILITIES or not has_value:
            raise FetchRefused(detail=f"The git endpoint does not relay the {shown(name)} capability.")


def check_arguments(rules: Sequence[FetchRule], arguments: list[str], id_length: int) -> frozenset[str]:
    """Refuse the arguments of a ``fetch`` command that could reach past the wants, and return the wants.

    ``id_length`` is the length of an object id in the request's object format. Raises FetchRefused, and
    GitMalformedRequestBody for an argument that doesn't hold what git sends in it.
    """
    wants: set[str] = set()
    for argument in arguments:
        if argument in _FETCH_FLAGS:
            continue
        name, _, value = argument.partition(" ")
        if name in ("want", "have", "shallow"):
            if not is_object_id(value, id_length):
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
        elif name == "deepen-since":
            if not _TIME.fullmatch(value):
                raise GitMalformedRequestBody(detail="The fetch's deepen-since line does not hold a time git sends.")
        elif name == "deepen-not":
            # upload-pack fails a deepen-not that names no ref, which would tell whether a ref the grant hides exists.
            if not covers(rules, value):
                raise FetchRefused(
                    detail=f"The git endpoint does not relay a deepen-not of {shown(value)}, which is not the full "
                    "name of a branch or tag the git grant covers. Name one in full, such as refs/heads/main."
                )
        elif name == "filter":
            if not _FILTER.fullmatch(value):
                raise FetchRefused(
                    detail=f"The git endpoint does not relay the filter {shown(value)} under a "
                    "grant whose fetch rules name branches and tags."
                )
        elif name in ("want-ref", "packfile-uris"):
            raise FetchRefused(
                detail=f"The git endpoint does not relay {name} under a grant whose fetch rules name branches and tags."
            )
        else:
            raise FetchRefused(detail=f"The git endpoint does not relay a fetch with {shown(name)}.")
    return frozenset(wants)
