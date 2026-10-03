"""Minimal length-prefixed JSON-over-TCP transport.

We deliberately avoid any RPC framework so the wire protocol itself is part
of the learning exercise: every message is a 4-byte big-endian length prefix
followed by that many bytes of UTF-8 JSON. Each connection carries exactly
one request and one response, which keeps the server simple at the cost of
a little TCP-handshake overhead -- fine for a teaching-scale cluster.
"""

import json
import socket
import struct
import threading


def _recv_exact(sock, n):
    """Read exactly n bytes from sock, or return None on EOF/short read."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


def send_frame(sock, obj):
    data = json.dumps(obj).encode("utf-8")
    sock.sendall(struct.pack(">I", len(data)) + data)


def recv_frame(sock):
    header = _recv_exact(sock, 4)
    if header is None:
        return None
    (length,) = struct.unpack(">I", header)
    data = _recv_exact(sock, length)
    if data is None:
        return None
    return json.loads(data.decode("utf-8"))


def call(host, port, message, timeout=0.5):
    """Send a request and wait for a response. Returns None on any failure
    (connection refused, timeout, peer down) rather than raising, since a
    down peer is an expected, routine condition in a distributed system."""
    try:
        with socket.create_connection((host, port), timeout=timeout) as sock:
            sock.settimeout(timeout)
            send_frame(sock, message)
            return recv_frame(sock)
    except (OSError, socket.timeout, ValueError):
        return None


class RPCServer:
    """Listens on (host, port) and hands each connection's single request to
    `handler`, which must return a JSON-serializable dict response."""

    def __init__(self, host, port, handler):
        self.host = host
        self.port = port
        self.handler = handler
        self._sock = None
        self._accept_thread = None
        self._running = False

    def start(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, self.port))
        self._sock.listen(64)
        self._running = True
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()

    def _accept_loop(self):
        while self._running:
            try:
                self._sock.settimeout(0.5)
                conn, _addr = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._handle_one, args=(conn,), daemon=True).start()

    def _handle_one(self, conn):
        with conn:
            try:
                msg = recv_frame(conn)
                if msg is None:
                    return
                response = self.handler(msg)
                send_frame(conn, response)
            except Exception as exc:  # noqa: BLE001 - a bad request shouldn't kill the server
                try:
                    send_frame(conn, {"success": False, "error": str(exc)})
                except OSError:
                    pass

    def stop(self):
        self._running = False
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=1.0)
