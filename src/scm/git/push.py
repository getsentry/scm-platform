"""Checking a push's ref updates, so GitServer can keep a push to its grant's push rules.

A push's request body lists the ref updates as pkt-lines, then holds the pack. ``check_push`` reads the updates
before GitServer looks anything up, refuses the push unless a rule allows every update, and otherwise returns a body
that sends the updates it read followed by the rest, which it never reads. Anything it doesn't recognise refuses the
push. gitprotocol-pack(5) describes the format, which receive-pack speaks in protocol versions 0 and 1 alike, the
only ones it has.
"""

import contextlib
import re
from collections.abc import Mapping, Sequence
from typing import NamedTuple

from scm.errors import GitMalformedRequestBody, GitOperationNotGranted, SCMCodedError
from scm.git.relay import DEFAULT_CHUNK_SIZE, _read
from scm.git.types import GrantV2PushRule, Readable, _is_branch_name

MAX_UPDATES_LENGTH = 64 * 1024
"""The most bytes a push may send before its pack. An update takes under 100 bytes plus its ref's name, and a push
from a shallow clone also lists the clone's shallow commits, about 50 bytes each."""

# The capabilities git asks for in a push, apart from push options, which ask the provider to act on the push, such
# as GitLab opening a merge request or skipping CI. Any other is refused, since it could change what a push does.
_CAPABILITIES = frozenset(
    {"atomic", "ofs-delta", "quiet", "report-status", "report-status-v2", "side-band", "side-band-64k"}
)
_CAPABILITIES_WITH_VALUES = frozenset({"agent", "object-format", "session-id"})
# The object formats git names in object-format=, and the length of an object id in each, in hex.
_OBJECT_ID_LENGTHS = {"sha1": 40, "sha256": 64}

_FLUSH = b"0000"
# A pkt-line's length, in 4 hex digits, counts the 4 digits too.
_PKT_LENGTH = re.compile(rb"[0-9a-fA-F]{4}")
_MAX_PKT_LENGTH = 65520
_OBJECT_ID = re.compile(rb"[0-9a-f]{40}|[0-9a-f]{64}")
# The most of a ref's name a refusal's detail shows. The pushing client chooses the name, and the detail reaches
# error reports and logs.
_SHOWN_REF_LENGTH = 200


class PushRefused(GitOperationNotGranted):
    """A push the grant does not allow, with the response that tells git why, if the client can read one."""

    def __init__(self, *, detail: str, report: bytes | None) -> None:
        super().__init__(detail=detail)
        self.report = report


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


def check_push(rules: Sequence[GrantV2PushRule], headers: Mapping[str, str], body: Readable | None) -> Readable:
    """Return the body to relay for a push that ``rules`` allow, and refuse any other.

    ``body`` must end where the request body ends. A refused push's body is read to the end first, because git reads
    the answer only once it has sent the whole push, and a connection closed before then shows git only a broken
    pipe. Raises PushRefused when no rule allows an update, or for a signed push, and GitMalformedRequestBody for a
    body that is missing or not a list of ref updates. A push with no rules is refused like any other.
    """
    if body is None:
        raise GitMalformedRequestBody(detail="The push has no body.")
    try:
        request = read_push_request(headers, body)
        if not request.updates and _read(body, 1):
            # git's probe before a large push is a lone flush, and a provider reads nothing after a flush that
            # updates no ref.
            raise GitMalformedRequestBody(detail="The push updates no ref but goes on after its ref updates.")
        refused = refused_updates(rules, request)
        if refused:
            ref, reason = next(iter(refused.items()))
            raise PushRefused(
                detail=f"The git grant does not allow this push: {_shown(ref)} ({reason}).",
                report=refusal_report(request, refused),
            )
    except (GitOperationNotGranted, GitMalformedRequestBody):
        _drain(body)
        raise
    return ReplayedBody(request.head, body)


def read_push_request(headers: Mapping[str, str], body: Readable) -> PushRequest:
    """Read a push's ref updates from ``body``, leaving the pack unread.

    Raises GitMalformedRequestBody for a body that does not start with a list of ref updates, GitOperationNotGranted
    for a compressed body, PushRefused for a signed push, whose updates come inside a certificate, and
    ClientClosedRequest if reading the body fails.
    """
    encoding = next((value for name, value in headers.items() if name.lower() == "content-encoding"), "identity")
    if encoding.strip().lower() not in ("", "identity"):
        # git compresses fetches but never pushes.
        raise GitOperationNotGranted(detail="The git endpoint does not read a compressed push.")
    reader = _PktReader(body)
    shallow: list[bytes] = []
    updates: list[RefUpdate] = []
    capabilities: frozenset[str] = frozenset()
    # Each line must be what git sends, in the order it sends them. receive-pack reads more than that, such as
    # capabilities on any update and shallow commits between updates, and anything this lets through it relays.
    while (line := reader.read_line()) is not None:
        if not updates and line.startswith(b"shallow "):
            # A push from a shallow clone lists its shallow commits first, and they pass on unchanged.
            shallow.append(line.removeprefix(b"shallow "))
            continue
        if not updates and line.startswith(b"push-cert\0"):
            message = "The git endpoint does not relay signed pushes."
            requested = _capabilities(line.partition(b"\0")[2])
            raise PushRefused(detail=message, report=_error_report(requested, message))
        line, separator, requested_bytes = line.partition(b"\0")
        if separator and updates:
            raise GitMalformedRequestBody(detail="Only the push's first ref update may carry capabilities.")
        if not updates:
            capabilities = _capabilities(requested_bytes)
        updates.append(_ref_update(line))

    formats = [name.partition("=")[2] for name in capabilities if name.startswith("object-format=")]
    if len(formats) > 1:
        raise GitMalformedRequestBody(detail="The push names more than one object format.")
    # Without object-format=, git means SHA-1.
    id_length = _OBJECT_ID_LENGTHS.get(formats[0] if formats else "sha1")
    if id_length is None:
        raise GitMalformedRequestBody(detail="The push names an object format git doesn't have.")
    if any(not _OBJECT_ID.fullmatch(commit) or len(commit) != id_length for commit in shallow):
        raise GitMalformedRequestBody(detail="The push lists a shallow commit that is not an object id.")
    if any(len(update.old_id) != id_length for update in updates):
        raise GitMalformedRequestBody(detail="The push's object ids don't match its object format.")
    if len({update.ref for update in updates}) < len(updates):
        raise GitMalformedRequestBody(detail="The push updates a ref more than once.")
    return PushRequest(updates=updates, capabilities=capabilities, head=reader.read_so_far())


def refused_updates(rules: Sequence[GrantV2PushRule], request: PushRequest) -> dict[bytes, str]:
    """Return why ``rules`` refuse each ref of ``request``, or nothing if every update has a rule that allows it.

    A push is relayed whole or not at all, so once one update is refused, every other ref gets a reason too. With no
    rules, every update is refused.
    """
    unsupported = sorted(name for name in request.capabilities if not _supported(name))
    if unsupported:
        reason = f"the git endpoint does not relay {_capability_name(unsupported[0])}"
        return {update.ref: reason for update in request.updates}
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
    # Reading the push checked that the branch's name is one git takes, so it decodes.
    if not rule.covers(update.ref.removeprefix(b"refs/heads/").decode()):
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
    return _on_side_band(request.capabilities, 1, report) or report


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
            chunk = _read(self.body, size - len(data))
            if not chunk:
                raise GitMalformedRequestBody(detail="The push ended before its ref updates did.")
            data += chunk
        self.read_bytes += data
        return bytes(data)


def _drain(body: Readable) -> None:
    with contextlib.suppress(OSError, SCMCodedError):
        while body.read(DEFAULT_CHUNK_SIZE):
            pass


def _supported(capability: str) -> bool:
    name, has_value, _ = capability.partition("=")
    return name in _CAPABILITIES_WITH_VALUES if has_value else name in _CAPABILITIES


def _capability_name(capability: str) -> str:
    if capability == "push-options":
        return "push options"
    return f"the {capability.partition('=')[0]} capability"


def _error_report(capabilities: frozenset[str], message: str) -> bytes | None:
    """Return a response that shows git ``message`` before it gives up, or None if the client can read none."""
    # Band 3 carries a message git prints as "remote: <message>" before it stops.
    return _on_side_band(capabilities, 3, f"{message}\n".encode())


def _on_side_band(capabilities: frozenset[str], band: int, data: bytes) -> bytes | None:
    """Return ``data`` on side band ``band``, ending with a flush, or None if the client asked for no side band."""
    if "side-band-64k" in capabilities:
        band_size = _MAX_PKT_LENGTH - 5
    elif "side-band" in capabilities:
        band_size = 1000 - 5
    else:
        return None
    return b"".join(_pkt(bytes([band]) + data[i : i + band_size]) for i in range(0, len(data), band_size)) + _FLUSH


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
    if ref.startswith(b"refs/heads/"):
        # A rule checks the branch the push names, and a name git wouldn't take might not be the branch the
        # provider updates.
        try:
            branch = ref.removeprefix(b"refs/heads/").decode()
        except UnicodeDecodeError:
            branch = ""
        if not _is_branch_name(branch):
            raise GitMalformedRequestBody(detail="The push updates a branch whose name git wouldn't take.")
    return RefUpdate(old_id=old_id, new_id=new_id, ref=ref)


def _shown(ref: bytes) -> str:
    name = ref.decode(errors="replace")
    return name if len(name) <= _SHOWN_REF_LENGTH else f"{name[:_SHOWN_REF_LENGTH]}..."


def _pkt(payload: bytes) -> bytes:
    return b"%04x" % (len(payload) + 4) + payload
