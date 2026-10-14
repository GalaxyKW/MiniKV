"""Bounded test proxy that observes backend replies without delivering them.

Captured replies are observer evidence, not successful HTTP client operations.
Releasing a gate closes its client connection; the listener remains available so
an unexpected gateway retry is still recorded, even after the engine is killed.
"""

from dataclasses import dataclass
import math
import socket
import struct
import threading
import time


@dataclass(frozen=True)
class ForwardAttempt:
    op: int
    key: bytes
    value: bytes


@dataclass(frozen=True)
class ResponseEntry:
    op: int
    key: bytes
    value: bytes
    status: int
    body: bytes


class ResponseGateProxy:
    """Forward RPC traffic, withholding PUT/DELETE replies for selected keys.

    Each frame has a five-second I/O budget and a held reply has a fifteen-second
    gate budget. The fixture also bounds lifetime connections and recorded frames;
    exceeding a limit is a test failure, never silent loss of retry evidence.
    """

    _IO_TIMEOUT = 5.0
    _GATE_TIMEOUT = 15.0
    _JOIN_TIMEOUT = 6.0
    _MAX_CONNECTIONS = 64
    _MAX_REQUESTS = 256

    def __init__(self, upstream, keys):
        host, port = upstream
        # Numeric IPv4 avoids an unbounded DNS lookup in this local test fixture.
        socket.inet_pton(socket.AF_INET, host)
        if type(port) is not int or not 1 <= port <= 65535:
            raise ValueError("upstream port must be between 1 and 65535")
        self.upstream = (host, port)
        self.keys = frozenset(keys)
        if any(not isinstance(key, bytes) for key in self.keys):
            raise TypeError("gated keys must be bytes")
        self.port = None
        self._condition = threading.Condition()
        self._stopping = threading.Event()
        self._released = threading.Event()
        self._listener = None
        self._accept_thread = None
        self._threads = []
        self._sockets = set()
        self._attempts = []
        self._responses = []
        self._errors = []

    def __enter__(self):
        if self._listener is not None:
            raise RuntimeError("a ResponseGateProxy cannot be restarted")
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.bind(("127.0.0.1", 0))
            listener.listen(16)
            listener.settimeout(0.1)
            self.port = listener.getsockname()[1]
            self._listener = listener
            self._accept_thread = threading.Thread(target=self._accept, daemon=True)
            self._accept_thread.start()
        except BaseException:
            listener.close()
            raise
        return self

    @property
    def errors(self):
        with self._condition:
            return tuple(self._errors)

    def check_errors(self):
        errors = self.errors
        if errors:
            raise RuntimeError("RPC fault proxy failed: " +
                               "; ".join(repr(error) for error in errors)) from errors[0]

    def attempts(self):
        with self._condition:
            return list(self._attempts)

    def snapshot(self):
        with self._condition:
            return list(self._responses)

    def wait_for_responses(self, count, timeout=3):
        if type(count) is not int or count < 0 or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("count must be nonnegative and timeout must be finite and positive")
        deadline = time.monotonic() + timeout
        with self._condition:
            while len(self._responses) < count:
                self.check_errors()
                if self._stopping.is_set():
                    raise RuntimeError("RPC fault proxy stopped before capturing responses")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("captured {} of {} backend responses; attempts={!r}".format(
                        len(self._responses), count, self._attempts))
                self._condition.wait(remaining)
            self.check_errors()
            return list(self._responses)

    def release(self):
        self._released.set()

    def _record_error(self, error):
        with self._condition:
            self._errors.append(error)
            self._condition.notify_all()

    def _register(self, sock):
        with self._condition:
            if self._stopping.is_set():
                sock.close()
                return False
            self._sockets.add(sock)
            return True

    def _close_socket(self, sock):
        if sock is not None:
            with self._condition:
                self._sockets.discard(sock)
            sock.close()

    @staticmethod
    def _read_exact(sock, length, deadline, idle=False):
        data = bytearray()
        while len(data) < length:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("RPC frame read exceeded its deadline")
            sock.settimeout(remaining)
            try:
                part = sock.recv(length - len(data))
            except socket.timeout:
                if idle and not data:
                    return None
                raise
            if not part:
                if idle and not data:
                    return None
                raise EOFError("connection closed within an RPC frame")
            data.extend(part)
        return bytes(data)

    def _request(self, client):
        deadline = time.monotonic() + self._IO_TIMEOUT
        header = self._read_exact(client, 16, deadline, idle=True)
        if header is None:
            return None
        magic, op, key_size, value_size = struct.unpack("!4sB3xII", header)
        if (magic != b"MKV1" or header[5:8] != b"\0\0\0" or
                op not in (1, 2, 3, 4) or key_size > 4096 or value_size > 1024 * 1024 or
                (op == 4 and (key_size != 0 or value_size != 0)) or
                (op != 4 and key_size == 0) or (op != 1 and value_size != 0)):
            raise ValueError("invalid RPC request header")
        payload = self._read_exact(client, key_size + value_size, deadline)
        request = ForwardAttempt(op, payload[:key_size], payload[key_size:])
        return request, header + payload

    def _response(self, upstream):
        deadline = time.monotonic() + self._IO_TIMEOUT
        header = self._read_exact(upstream, 12, deadline)
        magic, status, size = struct.unpack("!4sB3xI", header)
        if (magic != b"MKR1" or header[5:8] != b"\0\0\0" or status > 5 or
                size > 1024 * 1024 or (status in (0, 2) and size != 0)):
            raise ValueError("invalid RPC response header")
        body = self._read_exact(upstream, size, deadline)
        return status, body, header + body

    def _accept(self):
        try:
            while not self._stopping.is_set():
                try:
                    client, _ = self._listener.accept()
                except socket.timeout:
                    continue
                if not self._register(client):
                    return
                if len(self._threads) >= self._MAX_CONNECTIONS:
                    self._close_socket(client)
                    raise RuntimeError("RPC fault proxy connection limit exceeded")
                worker = threading.Thread(target=self._handle, args=(client,), daemon=True)
                self._threads.append(worker)
                try:
                    worker.start()
                except BaseException:
                    self._close_socket(client)
                    raise
        except OSError as error:
            if not self._stopping.is_set():
                self._record_error(error)
        except Exception as error:
            self._record_error(error)

    def _handle(self, client):
        upstream = None
        try:
            while not self._stopping.is_set():
                decoded = self._request(client)
                if decoded is None:
                    return
                request, wire = decoded
                # Record before connecting: an engine killed after the first
                # reply must not hide a retry whose upstream dial then fails.
                with self._condition:
                    if len(self._attempts) >= self._MAX_REQUESTS:
                        raise RuntimeError("RPC fault proxy request limit exceeded")
                    self._attempts.append(request)
                    self._condition.notify_all()
                if upstream is None:
                    upstream = socket.create_connection(self.upstream, timeout=self._IO_TIMEOUT)
                    if not self._register(upstream):
                        return
                upstream.settimeout(self._IO_TIMEOUT)
                upstream.sendall(wire)
                status, body, response = self._response(upstream)
                if request.op in (1, 3) and request.key in self.keys:
                    with self._condition:
                        self._responses.append(ResponseEntry(request.op, request.key, request.value,
                                                             status, body))
                        self._condition.notify_all()
                    if not self._released.wait(self._GATE_TIMEOUT):
                        raise TimeoutError("RPC response gate was not released")
                    return  # EOF, with no response bytes delivered to the gateway.
                client.settimeout(self._IO_TIMEOUT)
                client.sendall(response)
        except (OSError, EOFError) as error:
            if not self._stopping.is_set():
                self._record_error(error)
        except Exception as error:
            self._record_error(error)
        finally:
            self._close_socket(upstream)
            self._close_socket(client)

    def __exit__(self, exc_type, exc_value, traceback):
        self._stopping.set()
        self.release()
        self._listener.close()
        with self._condition:
            sockets = tuple(self._sockets)
            self._condition.notify_all()
        for sock in sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass  # Already disconnected sockets still need close().
            sock.close()
        deadline = time.monotonic() + self._JOIN_TIMEOUT
        self._accept_thread.join(max(0, deadline - time.monotonic()))
        for thread in self._threads:
            if thread.ident is not None:
                thread.join(max(0, deadline - time.monotonic()))
        if self._accept_thread.is_alive() or any(thread.is_alive() for thread in self._threads):
            self._record_error(RuntimeError("RPC fault proxy threads did not stop"))
        if self.errors and exc_value is not None:
            raise RuntimeError("RPC fault proxy failed during test failure: {!r}".format(
                self.errors)) from exc_value
        self.check_errors()
        return False
