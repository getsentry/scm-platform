"""What the endpoint asks the provider and reads from it, for a fetch kept to branches and tags: the refs the rules
cover, and the provider's list of capabilities without what the endpoint refuses."""

import io
from collections.abc import Generator, Mapping, Sequence
from typing import NamedTuple, Protocol

from scm.errors import GitProviderNotSupported, ResourceBadGateway
from scm.git.fetch.headers import header_values, replacing_header
from scm.git.fetch.policy import REFUSED_COMMANDS, REFUSED_FETCH_FEATURES, covers
from scm.git.pktline import DELIM, FLUSH, OBJECT_ID_LENGTHS, is_object_id, pkt, split_pkt_lines
from scm.git.relay import RelayResponse, ResponseBody, _printable
from scm.git.types import FetchRule, Readable

MAX_ADVERTISEMENT_LENGTH = 64 * 1024
"""The most bytes of the provider's list of capabilities the endpoint reads. Providers send under 1 KiB."""

MAX_REFS_LENGTH = 16 * 1024 * 1024
"""The most bytes of the provider's list of covered refs the endpoint reads. A rule with a branch pattern lists every
branch, about 100 bytes each."""


class Upstream(Protocol):
    """Sends a request to the repository's provider with Sentry's token, as ``GitRelay.forward`` does, and raises
    GitTokenRejected when the provider rejects the token."""

    def send(
        self, *, method: str, route: str, query: str, headers: Mapping[str, str], body: Readable | None
    ) -> RelayResponse: ...


class Refs(NamedTuple):
    """The refs a listing found that the rules cover."""

    lines: list[bytes]
    """The provider's line for each covered branch and tag, and for ``HEAD`` when it points to one of them."""
    tips: frozenset[str]
    """The object ids of the covered branches and tags, and of the objects their tags point to."""


def refs_request(rules: Sequence[FetchRule], object_format: str, *, unborn: bool = False) -> bytes:
    """Return the ``ls-refs`` request that lists ``HEAD`` and every branch and tag ``rules`` cover, and more.

    A prefix lists every ref whose name starts with it, so ``read_refs`` keeps only the refs a rule covers. With
    ``unborn``, it also lists a ``HEAD`` that names a branch with no commit yet, which an upload-pack older than git
    2.31 refuses to be asked for.
    """
    prefixes = {"HEAD"}
    for rule in rules:
        prefixes.update(rule.refs)
        if rule.branch_patterns:
            prefixes.add("refs/heads/")
        if rule.tag_patterns:
            prefixes.add("refs/tags/")
    arguments = [
        "symrefs",
        "peel",
        *(["unborn"] if unborn else []),
        *(f"ref-prefix {prefix}" for prefix in sorted(prefixes)),
    ]
    return (
        pkt(b"command=ls-refs\n")
        + pkt(f"object-format={object_format}\n".encode())
        + b"0001"
        + b"".join(pkt(f"{argument}\n".encode()) for argument in arguments)
        + b"0000"
    )


def refs_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Return the headers of the ``refs_request``, naming the client git's user agent as the client's request does."""
    agents = header_values(headers, "user-agent")
    return {
        "Git-Protocol": "version=2",
        "Content-Type": "application/x-git-upload-pack-request",
        "Accept": "application/x-git-upload-pack-result",
        "Accept-Encoding": "identity",
    } | ({"User-Agent": agents[-1]} if agents else {})


def list_refs(
    upstream: Upstream,
    rules: Sequence[FetchRule],
    headers: Mapping[str, str],
    object_format: str,
    *,
    unborn: bool = False,
) -> Refs:
    """Ask the provider for the refs ``rules`` cover. ``headers`` are the client's, for the user agent they name.

    Raises ResourceBadGateway if the provider doesn't answer with a list of refs.
    """
    response = upstream.send(
        method="POST",
        route="git-upload-pack",
        query="",
        headers=refs_headers(headers),
        body=io.BytesIO(refs_request(rules, object_format, unborn=unborn)),
    )
    try:
        if response.status_code != 200:
            raise ResourceBadGateway(
                detail=f"The provider answered HTTP {response.status_code} when the git endpoint listed the "
                "branches and tags the git grant covers. Try again, and if it keeps failing, check that Sentry's "
                "integration can still read this repository."
            )
        return read_refs(rules, response, object_format)
    finally:
        response.content.close()


def read_refs(rules: Sequence[FetchRule], response: RelayResponse, object_format: str) -> Refs:
    """Return the refs ``rules`` cover from the provider's answer to ``refs_request``.

    Raises ResourceBadGateway for an answer that isn't a list of refs.
    """
    _check_unencoded(response)
    data = _read_response(response.content, MAX_REFS_LENGTH, "list of branches and tags")
    id_length = OBJECT_ID_LENGTHS[object_format]
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
            if target is None or not covers(rules, target):
                continue
            # An empty repository's HEAD names a branch with no commit yet.
            ids = [] if object_id == "unborn" else [object_id]
        elif covers(rules, ref):
            ids = [object_id, *(value for key, value in attributes if key == "peeled")]
            tips.update(ids)
        else:
            continue
        if not all(is_object_id(tip, id_length) for tip in ids):
            raise ResourceBadGateway(detail="The provider listed a ref whose tip is not an object id.")
        lines.append(line)
    return Refs(lines=lines, tips=frozenset(tips))


def refs_response(refs: Refs) -> RelayResponse:
    """Return the ``ls-refs`` answer that lists ``refs``."""
    content = b"".join(pkt(line) for line in refs.lines) + b"0000"
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
        headers=replacing_header(response.headers, "Content-Length", str(len(content))),
        content=_in_memory(content),
    )


def _capabilities(data: bytes) -> bytes:
    """Return the provider's protocol version 2 list of capabilities without the ones the endpoint refuses."""
    lines = split_pkt_lines(data, ResourceBadGateway, "The provider's list of capabilities is not pkt-lines.")
    # The smart-HTTP header that names the service, which a server may leave out in version 2.
    if lines[:2] == [b"# service=git-upload-pack\n", FLUSH]:
        lines = lines[2:]
    if not lines or lines[0] not in (b"version 2\n", b"version 2") or lines[-1] != FLUSH or FLUSH in lines[:-1]:
        raise GitProviderNotSupported(
            detail="The provider did not answer in git protocol version 2, which the git endpoint needs to keep a "
            "fetch to the git grant's branches and tags."
        )
    kept: list[bytes] = [lines[0]]
    for line in lines[1:-1]:
        if not isinstance(line, bytes):
            raise ResourceBadGateway(detail="The provider's list of capabilities holds a delimiter.")
        name, has_value, value = line.removesuffix(b"\n").partition(b"=")
        if name in REFUSED_COMMANDS:
            continue
        if name == b"fetch" and has_value:
            features = [feature for feature in value.split(b" ") if feature not in REFUSED_FETCH_FEATURES]
            line = b"fetch=" + b" ".join(features) + b"\n"
        kept.append(line)
    return b"".join(pkt(line) for line in kept) + b"0000"


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


def _check_unencoded(response: RelayResponse) -> None:
    if any(value != "identity" for value in header_values(response.headers, "content-encoding")):
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
    lines = split_pkt_lines(data, ResourceBadGateway, "The provider's list of refs is not pkt-lines.")
    if not lines or lines[-1] != FLUSH or FLUSH in lines[:-1] or DELIM in lines:
        raise ResourceBadGateway(detail="The provider's list of refs does not end with its only flush.")
    texts = [line for line in lines if isinstance(line, bytes)]
    error = next((line for line in texts if line.startswith(b"ERR ")), None)
    if error is not None:
        message = _printable(error.removeprefix(b"ERR ").decode(errors="replace").strip(), 200)
        raise ResourceBadGateway(detail=f"The provider refused to list the repository's refs: {message}")
    return texts
