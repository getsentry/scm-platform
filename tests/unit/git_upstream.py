"""A local HTTPS server standing in for a provider's git endpoint in relay tests.

The relay only sends tokens over https, so the server uses a self-signed certificate made for the test run. A relay
trusts it with ``GitRelay(ca_bundle=ca_bundle())``.
"""

import base64
import datetime
import functools
import ipaddress
import ssl
import sys
import tempfile
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from requests.structures import CaseInsensitiveDict


@dataclass
class ReceivedRequest:
    method: str
    path: str
    headers: CaseInsensitiveDict[str]
    body: bytes


type Handler = Callable[["UpstreamHandler"], None]


@dataclass
class Upstream:
    url: str
    ca_bundle: str
    requests: list[ReceivedRequest] = field(default_factory=list)
    connections: int = 0
    handler: Handler | None = None


class UpstreamHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: "_Server"

    def setup(self) -> None:
        super().setup()
        self.server.upstream.connections += 1

    def do_GET(self) -> None:
        self._dispatch()

    def do_POST(self) -> None:
        self._dispatch()

    def _dispatch(self) -> None:
        handler = self.server.upstream.handler or echo
        handler(self)

    def body_chunks(self) -> Iterator[bytes]:
        """Yield the request body as it arrives, one HTTP chunk or one read at a time."""
        if self.headers.get("Transfer-Encoding") == "chunked":
            while (size := int(self._chunk_size_line(), 16)) > 0:
                yield self.rfile.read(size)
                self.rfile.readline()
            self.rfile.readline()
        else:
            remaining = int(self.headers.get("Content-Length", "0"))
            while remaining > 0:
                chunk = self.rfile.read1(remaining)  # type: ignore[attr-defined]
                remaining -= len(chunk)
                yield chunk

    def _chunk_size_line(self) -> bytes:
        line = self.rfile.readline()
        if not line:
            # The relay gave up on the request partway through its body, as some tests make it do.
            raise ConnectionResetError("the client closed the connection mid-body")
        return line.strip()

    def record(self, body: bytes) -> None:
        self.server.upstream.requests.append(
            ReceivedRequest(
                method=self.command, path=self.path, headers=CaseInsensitiveDict(self.headers.items()), body=body
            )
        )

    def respond(self, status: int, body: bytes, headers: dict[str, str] | None = None) -> None:
        self.send_response(status)
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def start_chunked(self, status: int, headers: dict[str, str] | None = None) -> None:
        self.send_response(status)
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()

    def write_chunk(self, data: bytes) -> None:
        self.wfile.write(f"{len(data):x}\r\n".encode() + data + b"\r\n")
        self.wfile.flush()

    def log_message(self, format: str, *args: object) -> None:
        pass


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    upstream: Upstream

    def handle_error(self, request: object, client_address: object) -> None:
        # Some tests have the relay drop the connection mid-request on purpose (a timeout, a short body), and the
        # assertions are on the relay's side, so a reset here is expected rather than worth a traceback.
        if isinstance(sys.exc_info()[1], ConnectionError):
            return
        super().handle_error(request, client_address)  # type: ignore[arg-type]


def echo(handler: UpstreamHandler) -> None:
    body = b"".join(handler.body_chunks())
    handler.record(body)
    handler.respond(200, body, {"Content-Type": "application/x-git-upload-pack-result"})


def basic_auth(username: str, password: str) -> str:
    return "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()


@functools.cache
def _certificate() -> tuple[str, str]:
    """Return the paths of a self-signed certificate for 127.0.0.1 and its key, made once per test run."""
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")])
    now = datetime.datetime.now(datetime.UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), critical=False)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    directory = Path(tempfile.mkdtemp(prefix="scm-git-upstream-"))
    cert_path, key_path = directory / "cert.pem", directory / "key.pem"
    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    )
    return str(cert_path), str(key_path)


def ca_bundle() -> str:
    """The path of the certificate every fake upstream in this test run serves."""
    return _certificate()[0]


class running_upstream:
    """Context manager that serves an ``Upstream`` on a free local port."""

    def __enter__(self) -> Upstream:
        cert_path, key_path = _certificate()
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(cert_path, key_path)
        self._server = _Server(("127.0.0.1", 0), UpstreamHandler)
        self._server.socket = context.wrap_socket(self._server.socket, server_side=True)
        host, port = self._server.server_address[:2]
        self._server.upstream = Upstream(url=f"https://{host!s}:{port}", ca_bundle=cert_path)
        self._thread = threading.Thread(target=self._server.serve_forever, args=(0.01,), daemon=True)
        self._thread.start()
        return self._server.upstream

    def __exit__(self, *exc: object) -> None:
        self._server.shutdown()
        self._server.server_close()
