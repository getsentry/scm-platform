"""A local HTTP/1.1 server standing in for a provider's git endpoint in relay tests."""

import base64
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

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
            while (size := int(self.rfile.readline().strip(), 16)) > 0:
                yield self.rfile.read(size)
                self.rfile.readline()
            self.rfile.readline()
        else:
            remaining = int(self.headers.get("Content-Length", "0"))
            while remaining > 0:
                chunk = self.rfile.read1(remaining)  # type: ignore[attr-defined]
                remaining -= len(chunk)
                yield chunk

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


def echo(handler: UpstreamHandler) -> None:
    body = b"".join(handler.body_chunks())
    handler.record(body)
    handler.respond(200, body, {"Content-Type": "application/x-git-upload-pack-result"})


def basic_auth(username: str, password: str) -> str:
    return "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()


class running_upstream:
    """Context manager that serves an ``Upstream`` on a free local port."""

    def __enter__(self) -> Upstream:
        self._server = _Server(("127.0.0.1", 0), UpstreamHandler)
        host, port = self._server.server_address[:2]
        self._server.upstream = Upstream(url=f"http://{host!s}:{port}")
        self._thread = threading.Thread(target=self._server.serve_forever, args=(0.01,), daemon=True)
        self._thread.start()
        return self._server.upstream

    def __exit__(self, *exc: object) -> None:
        self._server.shutdown()
        self._server.server_close()
