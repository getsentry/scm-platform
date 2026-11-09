"""Reading a push's ref updates, so GitServer can keep a push to its rule's branches.

A push's request body lists the ref updates as pkt-lines, then holds the pack. GitServer reads the updates before it
looks anything up, refuses the push unless the rule allows every update, and otherwise relays the updates it read
followed by the rest of the body, which it never reads. gitprotocol-pack(5) describes the format, which
receive-pack speaks in protocol versions 0 and 1 alike, the only ones it has.
"""

import re
from collections.abc import Mapping, Sequence
from typing import NamedTuple

from scm.errors import ClientClosedRequest, GitMalformedRequestBody, GitOperationNotGranted, RpcMalformedRequestHeaders
from scm.git.types import GrantV2PushRule, Readable

MAX_UPDATES_LENGTH = 64 * 1024
"""The most bytes a push may send before its pack. An update takes under 100 bytes plus its ref's name, and a push
from a shallow clone also lists the clone's shallow commits, about 50 bytes each."""

_FLUSH = b"0000"
# A pkt-line's length, in 4 hex digits, counts the 4 digits too.
_PKT_LENGTH = re.compile(rb"[0-9a-fA-F]{4}")
_MAX_PKT_LENGTH = 65520
_OBJECT_ID = re.compile(rb"[0-9a-f]{40}|[0-9a-f]{64}")


class RefUpdate(NamedTuple):
    old_id: bytes
    new_id: bytes
    ref: bytes

    @property
    def creates(self) -> bool:
        return not self.old_id.strip(b"0")

    @property
    def deletes(self) -> bool:
        return not self.new_id.strip(b"0")


class PushRequest(NamedTuple):
    updates: list[RefUpdate]
    capabilities: frozenset[str]
    """What the client asked for, from its first update."""
    head: bytes
    """The bytes read from the body, to send on before the rest of it."""


def read_push_request(headers: Mapping[str, str], body: Readable) -> PushRequest:
    """Read a push's ref updates from ``body``, leaving the pack unread.

    Raises GitMalformedRequestBody for a body that does not start with a list of ref updates, GitOperationNotGranted
    for a compressed body or a signed push, whose updates come inside a certificate, and ClientClosedRequest if
    reading the body fails.
    """
    encoding = next((value for name, value in headers.items() if name.lower() == "content-encoding"), "identity")
    if encoding.strip().lower() not in ("", "identity"):
        # git compresses fetches but never pushes.
        raise GitOperationNotGranted(detail="The git endpoint does not read a compressed push.")
    reader = _PktReader(body)
    updates: list[RefUpdate] = []
    capabilities: frozenset[str] = frozenset()
    while (line := reader.read_line()) is not None:
        if not updates and line.startswith(b"shallow "):
            # A push from a shallow clone lists its shallow commits first, and they pass on unchanged.
            if not _OBJECT_ID.fullmatch(line.removeprefix(b"shallow ")):
                raise GitMalformedRequestBody(detail="The push lists a shallow commit that is not an object id.")
            continue
        if not updates and line.startswith(b"push-cert"):
            raise GitOperationNotGranted(detail="The git endpoint does not relay signed pushes.")
        if not updates:
            line, _, requested = line.partition(b"\0")
            capabilities = _capabilities(requested)
        updates.append(_ref_update(line))
    return PushRequest(updates=updates, capabilities=capabilities, head=reader.read_so_far())


def refused_updates(rules: Sequence[GrantV2PushRule], request: PushRequest) -> dict[bytes, str]:
    """Return why ``rules`` refuse each ref of ``request``, or nothing if every update has a rule that allows it.

    A push is relayed whole or not at all, so once one update is refused, every other ref gets a reason too.
    """
    if "push-options" in request.capabilities:
        # Push options ask the provider to act on the push, such as GitLab opening a merge request or skipping CI.
        return {update.ref: "the git endpoint does not relay push options" for update in request.updates}
    refused: dict[bytes, str] = {}
    for update in request.updates:
        refusals = [_refusal(rule, update) for rule in rules]
        reasons = [refusal for refusal in refusals if refusal is not None]
        if len(reasons) == len(refusals):
            # The reason of the rule the update came closest to passing says the most.
            refused[update.ref] = max(reasons, default=_NOT_COVERED)[1]
    if refused:
        for update in request.updates:
            refused.setdefault(update.ref, "refused with the rest of the push")
    return refused


_NOT_A_BRANCH = (0, "the git grant allows pushing branches only")
_NOT_COVERED = (1, "the git grant does not cover this branch")
_NOT_NEW = (2, "the git grant allows only creating this branch")
_DELETES = (3, "the git grant does not allow deleting a branch")


def _refusal(rule: GrantV2PushRule, update: RefUpdate) -> tuple[int, str] | None:
    """Return why ``rule`` refuses ``update``, ranked by how far the update got, or None if the rule allows it."""
    if not update.ref.startswith(b"refs/heads/"):
        return _NOT_A_BRANCH
    try:
        branch = update.ref.removeprefix(b"refs/heads/").decode()
    except UnicodeDecodeError:
        return _NOT_COVERED
    if not rule.covers(branch):
        return _NOT_COVERED
    if update.deletes:
        return _DELETES
    if rule.create_only and not update.creates:
        return _NOT_NEW
    return None


def refusal_report(request: PushRequest, refused: Mapping[bytes, str]) -> bytes | None:
    """Return the response body that tells git why each ref was refused, or None if the client asked for none.

    git shows each ref as ``! [remote rejected] <branch> (<reason>)``, as for a provider's own refusal. A refused
    HTTP request would show only its status code.
    """
    if not {"report-status", "report-status-v2"} & request.capabilities:
        return None
    # "unpack ok" makes git show each ref's reason rather than an unpack failure. Each "ng" line is shorter than
    # the update it answers, so it fits in a pkt-line.
    report = (
        _pkt(b"unpack ok\n")
        + b"".join(_pkt(b"ng %s %s\n" % (update.ref, refused[update.ref].encode())) for update in request.updates)
        + _FLUSH
    )
    if "side-band-64k" in request.capabilities:
        band_size = _MAX_PKT_LENGTH - 5
    elif "side-band" in request.capabilities:
        band_size = 1000 - 5
    else:
        return report
    # On a side band the report travels on band 1, and the response ends with a flush of its own.
    return b"".join(_pkt(b"\1" + report[i : i + band_size]) for i in range(0, len(report), band_size)) + _FLUSH


def bounded_body(headers: Mapping[str, str], body: Readable) -> Readable:
    """Return ``body``, ending after its Content-Length if it has one, as the relay reads it.

    A caller may pass a stream that goes on past the body, such as a connection's socket, and reading the pushed
    updates or a refused pack past the body's end would wait for bytes that never come.
    """
    raw_length = next((value for name, value in headers.items() if name.lower() == "content-length"), None)
    if raw_length is None:
        return body
    try:
        length = int(raw_length)
    except ValueError:
        raise RpcMalformedRequestHeaders(detail="Could not parse Content-Length header") from None
    if length < 0:
        raise RpcMalformedRequestHeaders(detail="Could not parse Content-Length header")
    return _BoundedBody(body, length)


class _BoundedBody:
    def __init__(self, body: Readable, length: int) -> None:
        self.body = body
        self.remaining = length

    def read(self, size: int = -1, /) -> bytes:
        size = self.remaining if size < 0 else min(size, self.remaining)
        if size == 0:
            return b""
        data = self.body.read(size)
        self.remaining -= len(data)
        return data


class ReplayedBody:
    """A request body whose first bytes were read already, sending them before the rest."""

    def __init__(self, head: bytes, rest: Readable) -> None:
        self.head = head
        self.rest = rest

    def read(self, size: int = -1, /) -> bytes:
        if not self.head:
            return self.rest.read(size)
        if size < 0:
            data, self.head = self.head + self.rest.read(), b""
            return data
        data, self.head = self.head[:size], self.head[size:]
        return data


class _PktReader:
    def __init__(self, body: Readable) -> None:
        self.body = body
        self.read_bytes = bytearray()

    def read_line(self) -> bytes | None:
        """Return the next pkt-line's payload without its trailing newline, or None for a flush."""
        header = self._read(4)
        if not _PKT_LENGTH.fullmatch(header):
            raise GitMalformedRequestBody(detail="The push's ref updates are not pkt-lines.")
        length = int(header, 16)
        if length == 0:
            return None
        # 1 to 3 are protocol v2's special packets, which a push never sends.
        if length < 4 or length > _MAX_PKT_LENGTH:
            raise GitMalformedRequestBody(detail="The push's ref updates are not pkt-lines.")
        return self._read(length - 4).removesuffix(b"\n")

    def read_so_far(self) -> bytes:
        return bytes(self.read_bytes)

    def _read(self, size: int) -> bytes:
        if len(self.read_bytes) + size > MAX_UPDATES_LENGTH:
            raise GitMalformedRequestBody(detail=f"The push's ref updates are longer than {MAX_UPDATES_LENGTH} bytes.")
        data = bytearray()
        while len(data) < size:
            try:
                chunk = self.body.read(size - len(data))
            except OSError:
                raise ClientClosedRequest(detail="The client's connection failed while it sent its push.") from None
            if not chunk:
                raise GitMalformedRequestBody(detail="The push ended before its ref updates did.")
            data += chunk
        self.read_bytes += data
        return bytes(data)


def _capabilities(requested: bytes) -> frozenset[str]:
    try:
        return frozenset(requested.decode("ascii").split())
    except UnicodeDecodeError:
        raise GitMalformedRequestBody(detail="The push's capabilities are not ASCII.") from None


def _ref_update(line: bytes) -> RefUpdate:
    old_id, _, rest = line.partition(b" ")
    new_id, _, ref = rest.partition(b" ")
    if not (_OBJECT_ID.fullmatch(old_id) and _OBJECT_ID.fullmatch(new_id) and len(old_id) == len(new_id) and ref):
        raise GitMalformedRequestBody(detail="The push's ref updates are malformed.")
    return RefUpdate(old_id=old_id, new_id=new_id, ref=ref)


def _pkt(payload: bytes) -> bytes:
    return b"%04x" % (len(payload) + 4) + payload
