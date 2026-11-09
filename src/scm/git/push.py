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

from scm.errors import GitMalformedRequestBody, GitOperationNotGranted, RpcMalformedRequestHeaders, SCMCodedError
from scm.git.relay import DEFAULT_CHUNK_SIZE, _printable, _read
from scm.git.types import GrantV2PushRule, Readable, _follows_ref_format, _is_branch_name

MAX_UPDATES_LENGTH = 64 * 1024
"""The most bytes a push's ref updates may take. An update takes under 100 bytes plus its ref's name."""

MAX_SHALLOW_LENGTH = 4 * 1024 * 1024
"""The most bytes a push may take to list its shallow commits, which git sends before the updates of every push from
a shallow clone: about 79,000 SHA-1 commits at 53 bytes each. A clone of many branches at a small depth has many."""

# The capabilities git asks for in a push, apart from push options, which ask the provider to act on the push, such
# as GitLab opening a merge request or skipping CI. Any other is refused, since it could change what a push does.
_CAPABILITIES = frozenset({"atomic", "quiet", "report-status", "report-status-v2", "side-band-64k"})
_CAPABILITIES_WITH_VALUES = frozenset({"agent", "object-format", "session-id"})
# The object formats git names in object-format=, and the length of an object id in each, in hex.
_OBJECT_ID_LENGTHS = {"sha1": 40, "sha256": 64}
# What git puts in a capability list: printable ASCII, the capabilities split by spaces.
_CAPABILITY_LIST = re.compile(rb"[\x20-\x7e]*")
# The headers the relay passes on, with the only values git sends with each request of a push. git asks for no
# protocol version on a push, or for version 1, since a push has no version 2, and never compresses one.
_HEADER_VALUES = {
    "GET": {
        "accept": frozenset({"*/*"}),
        "content-encoding": frozenset(),
        "content-type": frozenset(),
        "git-protocol": frozenset({"version=1"}),
    },
    "POST": {
        "accept": frozenset({"application/x-git-receive-pack-result"}),
        "content-encoding": frozenset(),
        "content-type": frozenset({"application/x-git-receive-pack-request"}),
        "git-protocol": frozenset({"version=1"}),
    },
}
# The other headers the relay passes on, whose values vary with git's version and build. They may hold printable
# ASCII without surrounding space, which is what the relay sends.
_FREE_HEADERS = frozenset({"accept-encoding", "user-agent"})
_FREE_HEADER_VALUE = re.compile(r"[\x21-\x7e](?:[\x20-\x7e]*[\x21-\x7e])?")
# The longest branch name a push may update, in bytes. A rule's pattern runs on the name the client chose, and one
# that backtracks takes time that grows with the name's length.
_MAX_BRANCH_LENGTH = 255

_FLUSH = b"0000"
# A pkt-line's length, in 4 lowercase hex digits as git writes them, counts the 4 digits too.
_PKT_LENGTH = re.compile(rb"[0-9a-f]{4}")
_MAX_PKT_LENGTH = 65520
_OBJECT_ID = re.compile(rb"[0-9a-f]{40}|[0-9a-f]{64}")
# The most of a ref's name a refusal's detail shows. The pushing client chooses the name, and the detail reaches
# error reports and logs.
_SHOWN_REF_LENGTH = 200
_SHOWN_CAPABILITY_LENGTH = 40
# The most of a refused push's body read to its end. A provider takes no push this large, so a client that sends more
# only holds a worker, and git then sees the connection close instead of the reason.
_MAX_DRAIN_LENGTH = 256 * 1024 * 1024


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
    except (GitOperationNotGranted, GitMalformedRequestBody, RpcMalformedRequestHeaders):
        _drain(body)
        raise
    return ReplayedBody(request.head, body)


def read_push_request(headers: Mapping[str, str], body: Readable) -> PushRequest:
    """Read a push's ref updates from ``body``, leaving the pack unread.

    Raises GitMalformedRequestBody for a body that does not start with a list of ref updates,
    RpcMalformedRequestHeaders for a header value git doesn't send with a push, such as any Content-Encoding,
    PushRefused for a signed push, whose updates come inside a certificate, and ClientClosedRequest if reading the
    body fails.
    """
    check_push_headers("POST", headers)
    reader = _PktReader(body, limit=MAX_SHALLOW_LENGTH)
    shallow: list[bytes] = []
    updates: list[RefUpdate] = []
    capabilities: frozenset[str] = frozenset()
    # Each line must be what git sends, in the order it sends them. receive-pack reads more than that, such as
    # capabilities on any update and shallow commits between updates, and anything this lets through it relays.
    while (line := reader.read_line()) is not None:
        if updates:
            if b"\0" in line:
                raise GitMalformedRequestBody(detail="Only the push's first ref update may carry capabilities.")
            updates.append(_ref_update(line))
        elif line.startswith(b"shallow "):
            # A push from a shallow clone lists its shallow commits first, each on a line of its own, and they pass
            # on unchanged.
            if not line.endswith(b"\n"):
                raise GitMalformedRequestBody(detail="The push lists a shallow commit that is not an object id.")
            shallow.append(line.removeprefix(b"shallow ").removesuffix(b"\n"))
        elif line.startswith(b"push-cert\0"):
            message = "The git endpoint does not relay signed pushes."
            requested = _capabilities(line.partition(b"\0")[2])
            # Band 3 carries a message git prints as "remote: <message>" before it stops.
            raise PushRefused(detail=message, report=_on_side_band(requested, 3, f"{message}\n".encode()))
        else:
            # The updates' own budget starts here, apart from the shallow commits'.
            reader.limit_to(reader.line_start + MAX_UPDATES_LENGTH)
            update, _, requested_bytes = line.partition(b"\0")
            capabilities = _capabilities(requested_bytes)
            updates.append(_ref_update(update))

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


def check_push_headers(method: str, headers: Mapping[str, str]) -> None:
    """Refuse a push's request unless each header the relay passes on holds a value git sends with it.

    Every value counts, since a mapping can hold one header under two spellings and the relay passes on one of them.
    Raises RpcMalformedRequestHeaders.
    """
    allowed_values = _HEADER_VALUES[method]
    for name, value in headers.items():
        lowered = name.lower()
        allowed = allowed_values.get(lowered)
        if (allowed is not None and value not in allowed) or (
            lowered in _FREE_HEADERS and not _FREE_HEADER_VALUE.fullmatch(value)
        ):
            raise RpcMalformedRequestHeaders(detail=f"The push's {lowered} header holds a value git doesn't send.")


def refused_updates(rules: Sequence[GrantV2PushRule], request: PushRequest) -> dict[bytes, str]:
    """Return why ``rules`` refuse each ref of ``request``, or nothing if every update has a rule that allows it.

    A push is relayed whole or not at all, so once one update is refused, every other ref gets a reason too. With no
    rules, every update is refused.
    """
    unsupported = min((name for name in request.capabilities if not _supported(name)), default=None)
    if unsupported is not None:
        reason = f"the git endpoint does not relay {_capability_name(unsupported)}"
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
    def __init__(self, body: Readable, *, limit: int) -> None:
        self.body = body
        self.limit = limit
        self.read_bytes = bytearray()
        self.line_start = 0
        """Where the line read last starts, counted from the start of the body."""

    def read_line(self) -> bytes | None:
        """Return the next pkt-line's payload as sent, or None for a flush."""
        self.line_start = len(self.read_bytes)
        header = self._read(4)
        if not _PKT_LENGTH.fullmatch(header):
            raise GitMalformedRequestBody(detail="The push's ref updates are not pkt-lines.")
        length = int(header, 16)
        if length == 0:
            return None
        # 1 to 3 are protocol v2's special packets, which a push never sends.
        if not 4 <= length <= _MAX_PKT_LENGTH:
            raise GitMalformedRequestBody(detail="The push's ref updates are not pkt-lines.")
        return self._read(length - 4)

    def limit_to(self, limit: int) -> None:
        """Lower how far into the body the reader may read, refusing a body already read past it."""
        self.limit = min(self.limit, limit)
        self._check(0)

    def read_so_far(self) -> bytes:
        return bytes(self.read_bytes)

    def _check(self, size: int) -> None:
        if len(self.read_bytes) + size > self.limit:
            raise GitMalformedRequestBody(detail="The push's ref updates or shallow commits are too long.")

    def _read(self, size: int) -> bytes:
        self._check(size)
        data = bytearray()
        while len(data) < size:
            chunk = _read(self.body, size - len(data))
            if not chunk:
                raise GitMalformedRequestBody(detail="The push ended before its ref updates did.")
            data += chunk
        self.read_bytes += data
        return bytes(data)


def _drain(body: Readable) -> None:
    drained = 0
    with contextlib.suppress(OSError, SCMCodedError):
        while drained < _MAX_DRAIN_LENGTH and (chunk := body.read(DEFAULT_CHUNK_SIZE)):
            drained += len(chunk)


def _supported(capability: str) -> bool:
    name, has_value, _ = capability.partition("=")
    return name in _CAPABILITIES_WITH_VALUES if has_value else name in _CAPABILITIES


def _capability_name(capability: str) -> str:
    if capability == "push-options":
        return "push options"
    # The client chose the name, and the reason goes in every refused ref's line of the report.
    return f"the {_printable(capability.partition('=')[0], _SHOWN_CAPABILITY_LENGTH)} capability"


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
    if not _CAPABILITY_LIST.fullmatch(requested):
        raise GitMalformedRequestBody(detail="The push's capabilities hold characters git doesn't send.")
    return frozenset(requested.decode("ascii").split(" ")) - {""}


def _ref_update(line: bytes) -> RefUpdate:
    old_id, _, rest = line.partition(b" ")
    new_id, _, ref = rest.partition(b" ")
    if not (_OBJECT_ID.fullmatch(old_id) and _OBJECT_ID.fullmatch(new_id) and len(old_id) == len(new_id) and ref):
        raise GitMalformedRequestBody(detail="The push's ref updates are malformed.")
    # A rule checks the ref the push names, and a name git wouldn't take might not be the ref the provider updates.
    # It also goes into the refusal's detail, which reaches logs, so it must be printable too, which keeps out
    # invisible and right-to-left characters that make one branch name look like another.
    try:
        name = ref.decode()
    except UnicodeDecodeError:
        name = ""
    if not (name.startswith("refs/") and name.isprintable() and _follows_ref_format(name)):
        raise GitMalformedRequestBody(detail="The push updates a ref whose name git wouldn't take.")
    if name.startswith("refs/heads/"):
        if not _is_branch_name(name.removeprefix("refs/heads/")):
            raise GitMalformedRequestBody(detail="The push updates a branch whose name git wouldn't take.")
        if len(ref.removeprefix(b"refs/heads/")) > _MAX_BRANCH_LENGTH:
            raise GitMalformedRequestBody(
                detail=f"The push updates a branch whose name is longer than {_MAX_BRANCH_LENGTH} bytes."
            )
    return RefUpdate(old_id=old_id, new_id=new_id, ref=ref)


def _shown(ref: bytes) -> str:
    return _printable(ref.decode(errors="replace"), _SHOWN_REF_LENGTH)


def _pkt(payload: bytes) -> bytes:
    return b"%04x" % (len(payload) + 4) + payload
