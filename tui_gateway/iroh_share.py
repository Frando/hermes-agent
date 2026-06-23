"""Share a tui_gateway session over iroh at the JSON-RPC protocol level.

This is the host-side acceptor. When the gateway starts in share mode (see
``entry._install_iroh_share``) it binds an iroh endpoint and accepts joiner
connections. Each joiner gets a transport into the running gateway and attaches
to the host's live session, so it receives the same structured event stream the
host's TUI does and, with a control ticket, can drive the agent. It is the iroh
analogue of ``tui_gateway.ws``: every request flows through
``server.dispatch`` unchanged.

Two tokens are minted at startup, a watch token and a control token, embedded
in two tickets. A joiner presents its token in a ``hello``; the host grants the
matching role. Watch joiners may only call read-only methods; control joiners
may drive the agent while they hold control, taken with ``/grab``.

The module depends only on the standard library plus iroh (imported lazily) and
on ``tui_gateway.server``; iroh is optional, so a gateway without it still runs.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import secrets
import threading
from typing import Optional

from tui_gateway import server

logger = logging.getLogger(__name__)

ALPN = b"hermes-share/0"
# A ticket is "<endpoint-id>/<token>": the 64-hex endpoint id (resolved to
# addresses by iroh discovery, so no addresses are embedded) and a role token.
# Neither part contains a slash, so the last slash separates them.
_TICKET_SEP = "/"
_READ_CHUNK = 64 * 1024
_MAX_LINE_BYTES = 1024 * 1024
_MAX_CLIENTS = 32
_HELLO_TIMEOUT = 10.0
_ONLINE_TIMEOUT = 8.0
_MAX_QUEUED = 2000
# Join bridge: cap host frames buffered before the local TUI connects, and tear
# the bridge down if the TUI never connects, so a failed launch cannot leak an
# unbounded buffer or a phantom joiner on the host.
_MAX_BRIDGE_BUFFER = 2000
_BRIDGE_CONNECT_TIMEOUT = 60.0

ROLE_WATCH = "watch"
ROLE_CONTROL = "control"

# Methods a watch joiner may call. Deliberately tiny and audited: each entry is
# read-only AND scoped to the joiner's pinned session (or carries no session
# state at all). Everything else (including any method added later) is treated
# as mutating, so it is denied to watchers and gated behind control for
# controllers. Failing closed is the point. In particular this set must NOT
# include methods that take a non-session_id locator (so session pinning cannot
# apply), return secrets, run an agent, or read across sessions: e.g.
# preview.restart (spawns an agent), config.get/config.show (api keys),
# spawn_tree.list/load (cross-session, keyed by path), complete.path (filesystem).
_READ_ONLY_METHODS = frozenset({
    "commands.catalog",   # static slash-command catalog
    "command.resolve",    # resolve a command name
    "complete.slash",     # slash-command completion (no filesystem)
    "session.resume",     # attach to the pinned session (pinned-checked)
    "session.history",    # transcript of the pinned session (pinned-checked)
    "session.status",     # status of the pinned session (pinned-checked)
    "session.usage",      # token usage of the pinned session (pinned-checked)
})


class ShareError(RuntimeError):
    """A sharing operation failed."""


class ShareUnavailable(ShareError):
    """The optional ``iroh`` dependency is not installed."""


_iroh_log_configured = False


def _maybe_configure_iroh_logging(iroh) -> None:
    """Turn on iroh's own tracing when ``HERMES_IROH_LOG`` names a level.

    iroh logs to stderr, which for the gateway is the log channel (stdout is
    reserved for JSON-RPC), so this is a safe opt-in debug aid for connection
    problems. Accepts off/error/warn/info/debug/trace; anything else is ignored.
    """
    global _iroh_log_configured
    if _iroh_log_configured:
        return
    _iroh_log_configured = True
    level = (os.environ.get("HERMES_IROH_LOG") or "").strip().upper()
    if not level or not hasattr(iroh.LogLevel, level):
        return
    try:
        iroh.set_log_level(getattr(iroh.LogLevel, level))
        logger.debug("iroh: log level set to %s (stderr)", level)
    except Exception:
        logger.debug("iroh: set_log_level failed", exc_info=True)


def _require_iroh():
    try:
        import iroh
    except ImportError as exc:
        raise ShareUnavailable(
            "Session sharing needs the 'iroh' package (a core dependency). "
            "Reinstall hermes-agent, or: pip install iroh"
        ) from exc
    _maybe_configure_iroh_logging(iroh)
    return iroh


def _split_ticket(shared: str) -> tuple[str, str]:
    shared = shared.strip()
    if _TICKET_SEP not in shared:
        raise ValueError("not a valid share ticket (missing token)")
    base, token = shared.rsplit(_TICKET_SEP, 1)
    if not base or not token:
        raise ValueError("not a valid share ticket")
    return base, token


def _encode_id(endpoint_id) -> str:
    """Encode an EndpointId as lowercase base32 with no padding (52 chars).

    Shorter than the 64-char hex string and the iroh FFI exposes no base32
    encoder, so encode the raw 32 bytes here.
    """
    return base64.b32encode(endpoint_id.to_bytes()).decode("ascii").rstrip("=").lower()


def _decode_id(iroh, encoded: str):
    """Inverse of :func:`_encode_id`: base32 string -> EndpointId."""
    padded = encoded.strip().upper()
    padded += "=" * (-len(padded) % 8)
    return iroh.EndpointId.from_bytes(base64.b32decode(padded))


def _rpc_error(rid, code: int, message: str) -> dict:
    """A JSON-RPC error response frame."""
    return {"jsonrpc": "2.0", "id": rid, "error": {"code": code, "message": message}}


def _connect_addr(iroh, endpoint_id: str):
    """Build the iroh address to dial from a ticket's base32 endpoint id.

    Only the endpoint id is carried in a ticket; its actual addresses are
    resolved by iroh discovery at connect time. Tests monkeypatch this to return
    a known direct address so they can connect over loopback without discovery.
    """
    return iroh.EndpointAddr(_decode_id(iroh, endpoint_id), None, [])


class _LineReader:
    """Reassemble newline-delimited JSON messages from a recv stream."""

    def __init__(self, recv):
        self._recv = recv
        self._buf = b""

    async def next(self) -> Optional[dict]:
        while True:
            nl = self._buf.find(b"\n")
            if nl >= 0:
                line = self._buf[:nl].strip()
                self._buf = self._buf[nl + 1:]
                if not line:
                    continue
                try:
                    return json.loads(line.decode("utf-8"))
                except (ValueError, UnicodeDecodeError):
                    continue
            if len(self._buf) > _MAX_LINE_BYTES:
                logger.debug("share: dropping over-size message from peer")
                return None
            try:
                chunk = await self._recv.read(_READ_CHUNK)
            except Exception:
                return None
            if not chunk:
                return None
            self._buf += chunk


class _ClientTransport:
    """A gateway Transport that writes JSON-RPC frames to one iroh joiner.

    ``write`` is called from gateway worker threads (and the acceptor loop), so
    it enqueues onto the iroh event loop and a writer task drains to the send
    stream. Event frames for sessions other than the one this joiner attached
    to are dropped, so a joiner observing one shared session never sees the
    activity of the host's other sessions.
    """

    def __init__(self, send, loop: asyncio.AbstractEventLoop, attached_sid: Optional[str]):
        self._send = send
        self._loop = loop
        self._attached_sid = attached_sid
        self._queue: asyncio.Queue = asyncio.Queue()
        self._closed = False
        self._writer = loop.create_task(self._drain())

    def write(self, obj: dict) -> bool:
        if self._closed:
            return False
        if not self._allowed(obj):
            return True  # filtered, but the connection is healthy
        try:
            # Do the bound-check, drop, and enqueue together on the loop thread.
            # asyncio.Queue is not safe to size/drain from another thread, so
            # write() (called from gateway worker threads) must not touch it
            # directly.
            self._loop.call_soon_threadsafe(self._enqueue, obj)
        except RuntimeError:
            return False
        return True

    def _enqueue(self, obj: dict) -> None:
        if self._closed:
            return  # a write scheduled just before close must not enqueue
        if self._queue.qsize() >= _MAX_QUEUED:
            try:
                self._queue.get_nowait()  # drop oldest; runs on the loop thread
            except asyncio.QueueEmpty:
                pass
        self._queue.put_nowait(obj)

    def _allowed(self, obj: dict) -> bool:
        return _frame_for_sid(obj, self._attached_sid)

    async def _drain(self) -> None:
        while True:
            obj = await self._queue.get()
            if obj is None:
                return
            try:
                data = (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")
                await self._send.write_all(data)
            except Exception as exc:
                logger.debug("share: client write failed: %r", exc)
                self._closed = True
                return

    def close(self) -> None:
        self._closed = True
        try:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, None)
        except RuntimeError:
            pass


class _SharedSession:
    """One session the host has shared, with its own tokens and controller.

    The endpoint is shared across all shared sessions, but each session gets its
    own watch and control tokens and tracks its own controller, so control is
    negotiated independently per session.
    """

    __slots__ = (
        "key", "sid", "watch_token", "control_token",
        "watch_ticket", "control_ticket", "controller",
    )

    def __init__(self, key: str, sid: str, watch_token: str, control_token: str,
                 watch_ticket: str, control_ticket: str):
        self.key = key
        self.sid = sid
        self.watch_token = watch_token
        self.control_token = control_token
        self.watch_ticket = watch_ticket
        self.control_ticket = control_ticket
        # None means the host (the local pane) drives this session; a _Client
        # means that joiner holds control. Only one party drives at a time.
        self.controller: Optional["_Client"] = None


class _Client:
    __slots__ = ("cid", "name", "role", "transport", "shared", "conn")

    def __init__(self, cid: str, name: str, role: str, transport: _ClientTransport,
                 shared: _SharedSession):
        self.cid = cid
        self.name = name
        self.role = role
        self.transport = transport
        self.shared = shared
        self.conn = None

    # The joiner addresses the shared session two ways: the ephemeral id on
    # events and prompt.submit, and the resume key on session.resume. Both are
    # allowed; anything else is a different session and refused.
    @property
    def pinned_sid(self) -> str:
        return self.shared.sid

    @property
    def pinned_key(self) -> str:
        return self.shared.key


class IrohShareHost:
    """Accept iroh joiners and bridge them into the running gateway."""

    def __init__(self):
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._endpoint = None
        self._endpoint_b32: Optional[str] = None
        self._clients: dict[str, _Client] = {}
        # Per-session registry, guarded by _reg_lock because it is touched from
        # the gateway thread (share/unshare) and the acceptor loop (hello/grab).
        self._shared: dict[str, _SharedSession] = {}   # session_key -> shared
        self._by_token: dict[str, tuple[_SharedSession, str]] = {}  # token -> (shared, role)
        self._reg_lock = threading.Lock()
        self._ready = threading.Event()
        self._error: Optional[BaseException] = None
        self._closing = False
        self._started = False
        self._start_lock = threading.Lock()

    # -- endpoint lifecycle (bound lazily on the first share) --------------

    def ensure_started(self, online_timeout: float = _ONLINE_TIMEOUT) -> None:
        """Bind the iroh endpoint once, lazily. Raise ShareError on failure.

        The endpoint is process-wide and shared by every shared session, so it
        is bound the first time any session is shared rather than at startup,
        which keeps iroh entirely out of the picture for users who never share.
        """
        with self._start_lock:
            if self._started:
                return
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._run, args=(online_timeout,),
                    name="hermes-tui-share", daemon=True,
                )
                self._thread.start()
            self._ready.wait(timeout=online_timeout + 15)
            if self._error is not None:
                raise ShareError(f"failed to start sharing: {self._error}") from self._error
            if self._endpoint_b32 is None:
                raise ShareError("sharing endpoint did not start in time")
            self._started = True

    @property
    def shared_count(self) -> int:
        with self._reg_lock:
            return len(self._shared)

    def is_shared(self, key: str) -> bool:
        with self._reg_lock:
            return key in self._shared

    def session_tickets(self, key: str) -> Optional[tuple[str, str]]:
        with self._reg_lock:
            shared = self._shared.get(key)
            return (shared.watch_ticket, shared.control_ticket) if shared else None

    def stop(self) -> None:
        self._closing = True
        loop = self._loop
        if loop is not None:
            try:
                loop.call_soon_threadsafe(self._begin_shutdown)
            except RuntimeError:
                pass
        if self._thread is not None:
            self._thread.join(timeout=5)

    # -- share / unshare (called on the gateway thread) --------------------

    def share_session(self, sid: str, key: str,
                      online_timeout: float = _ONLINE_TIMEOUT) -> tuple[str, str]:
        """Share the session, returning ``(watch_ticket, control_ticket)``.

        Idempotent: re-sharing an already-shared session returns the same
        tickets and refreshes the ephemeral id in case the live session was
        rebound to a new one.
        """
        self.ensure_started(online_timeout)
        with self._reg_lock:
            shared = self._shared.get(key)
            if shared is not None:
                shared.sid = sid
                return shared.watch_ticket, shared.control_ticket
            base = self._endpoint_b32
            watch_token = secrets.token_urlsafe(12)
            control_token = secrets.token_urlsafe(12)
            shared = _SharedSession(
                key, sid, watch_token, control_token,
                f"{base}{_TICKET_SEP}{watch_token}",
                f"{base}{_TICKET_SEP}{control_token}",
            )
            self._shared[key] = shared
            self._by_token[watch_token] = (shared, ROLE_WATCH)
            self._by_token[control_token] = (shared, ROLE_CONTROL)
            return shared.watch_ticket, shared.control_ticket

    def share_active_session(
        self, online_timeout: float = _ONLINE_TIMEOUT
    ) -> Optional[tuple[str, str]]:
        """Share the gateway's most recently active live session.

        A convenience over :meth:`share_session` for the ``hermes share``
        auto-start path and tests: it resolves the active session (ensuring its
        DB row) and shares it. Returns the tickets, or None when there is no live
        session to share.
        """
        handle = server.shared_session_handle()
        if handle is None:
            return None
        sid, key = handle
        return self.share_session(sid, key, online_timeout)

    def unshare_session(self, key: str) -> bool:
        """Stop sharing the session and disconnect its joiners.

        Returns False when the session was not shared.
        """
        with self._reg_lock:
            shared = self._shared.pop(key, None)
            if shared is None:
                return False
            self._by_token.pop(shared.watch_token, None)
            self._by_token.pop(shared.control_token, None)
        loop = self._loop
        if loop is not None:
            try:
                loop.call_soon_threadsafe(self._drop_session_clients, key)
            except RuntimeError:
                pass
        return True

    def _drop_session_clients(self, key: str) -> None:
        # Runs on the acceptor loop: notify and disconnect the joiners of an
        # unshared session. Closing the connection ends each read loop, whose
        # finally then prunes the client and its transport.
        for client in list(self._clients.values()):
            if client.shared.key != key:
                continue
            try:
                client.transport.write({
                    "jsonrpc": "2.0", "method": "event", "params": {
                        "type": "notification.show",
                        "payload": {"text": "[share] the host stopped sharing this session.",
                                    "kind": "ttl", "ttl_ms": 6000, "level": "warn"},
                    },
                })
            except Exception:
                pass
            conn = client.conn
            if conn is not None:
                try:
                    conn.close(0, b"unshared")
                except Exception:
                    pass

    def _run(self, online_timeout: float) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._bind(online_timeout))
        except BaseException as exc:  # noqa: BLE001 - surfaced via ensure_started()
            self._error = exc
            self._ready.set()
            self._loop.close()
            return
        self._ready.set()
        try:
            self._loop.run_forever()
        finally:
            self._loop.close()

    async def _bind(self, online_timeout: float) -> None:
        iroh = _require_iroh()
        options = iroh.EndpointOptions(preset=iroh.preset_n0(), alpns=[ALPN])
        self._endpoint = await iroh.Endpoint.bind(options)
        try:
            await asyncio.wait_for(self._endpoint.online(), timeout=online_timeout)
        except Exception:
            pass
        self._endpoint_b32 = _encode_id(self._endpoint.id())
        self._loop.create_task(self._accept_loop())

    def _begin_shutdown(self) -> None:
        self._loop.create_task(self._shutdown_and_stop())

    async def _shutdown_and_stop(self) -> None:
        self._closing = True
        try:
            if self._endpoint is not None:
                await self._endpoint.close()
        except Exception:
            pass
        # Cancel the accept loop, per-connection handlers, and writer tasks so
        # the loop has no pending work (and no iroh continuation fires) when it
        # closes.
        current = asyncio.current_task()
        pending = [t for t in asyncio.all_tasks(self._loop) if t is not current]
        for task in pending:
            task.cancel()
        if pending:
            # Await the cancellations so each task runs its finally/cleanup (and
            # any iroh continuation settles) before the loop closes.
            await asyncio.gather(*pending, return_exceptions=True)
        self._loop.stop()

    # -- accept + per-connection ------------------------------------------

    async def _accept_loop(self) -> None:
        while not self._closing:
            try:
                incoming = await self._endpoint.accept_next()
            except Exception:
                if self._closing:
                    break
                logger.debug("share: accept_next failed, retrying", exc_info=True)
                await asyncio.sleep(0.05)
                continue
            if incoming is None:
                if self._closing:
                    break
                await asyncio.sleep(0.05)
                continue
            self._loop.create_task(self._handle(incoming))

    async def _handle(self, incoming) -> None:
        conn = None
        client = None
        transport = None
        try:
            conn = await (await incoming.accept()).connect()
            bi = await conn.accept_bi()
        except Exception:
            logger.debug("share: connection handshake failed", exc_info=True)
            return
        recv, send = bi.recv(), bi.send()
        reader = _LineReader(recv)
        try:
            try:
                hello = await asyncio.wait_for(reader.next(), timeout=_HELLO_TIMEOUT)
            except Exception:
                return
            # The handshake is a JSON-RPC exchange: the joiner sends a `hello`
            # request, the host replies with the welcome as its result (or an
            # error). After that the stream is ordinary JSON-RPC.
            if not isinstance(hello, dict) or hello.get("method") != "hello":
                rid = hello.get("id") if isinstance(hello, dict) else None
                await self._raw_write(send, _rpc_error(rid, -32600, "expected hello"))
                return
            rid = hello.get("id")
            params = hello.get("params") if isinstance(hello.get("params"), dict) else {}
            shared, role = self._lookup_token(params.get("token") or "")
            if shared is None:
                # The token matches no shared session: either the ticket is
                # wrong or the host has stopped sharing that session.
                await self._raw_write(send, _rpc_error(rid, 4030, "invalid ticket"))
                return
            if len(self._clients) >= _MAX_CLIENTS:
                await self._raw_write(send, _rpc_error(rid, 4290, "session full"))
                return
            name = _sanitize((params.get("name") or "guest")).strip()[:40] or "guest"

            # The token determines the session: events and prompt.submit use the
            # ephemeral id (so the event filter and submit pin track it); the
            # joiner resumes by the resume key.
            transport = _ClientTransport(send, self._loop, shared.sid)
            client = _Client(secrets.token_hex(4), name, role, transport, shared)
            client.conn = conn

            # Send the welcome (the hello response) and gateway.ready FIRST, then
            # attach to the fan-out. Attaching last guarantees the joiner cannot
            # receive a session event ahead of gateway.ready. The welcome carries
            # the RESUME KEY so the remote TUI's session.resume reattaches to this
            # exact live session (resume is keyed by the persistent key, not the
            # ephemeral id) and gets the ephemeral id back to drive it with.
            await self._raw_write(send, {
                "jsonrpc": "2.0", "id": rid,
                "result": {
                    "role": role, "session_id": shared.key,
                    "controller": self._holder_name(shared),
                },
            })
            await self._raw_write(send, {
                "jsonrpc": "2.0", "method": "event",
                "params": {"type": "gateway.ready", "payload": {"skin": server.resolve_skin()}},
            })
            self._clients[client.cid] = client
            server.attach_shared_transport(transport)
            self._notice(shared, f"{name} joined ({role})")
            await self._read_loop(client, reader)
        finally:
            if client is not None:
                self._clients.pop(client.cid, None)
                if client.shared.controller is client:
                    # The controller left; control returns to the host.
                    self._set_controller(client.shared, None)
                self._notice(client.shared, f"{client.name} left")
            if transport is not None:
                server.detach_shared_transport(transport)
                transport.close()
            if conn is not None:
                try:
                    conn.close(0, b"bye")
                except Exception:
                    pass

    async def _read_loop(self, client: _Client, reader: _LineReader) -> None:
        while not self._closing:
            req = await reader.next()
            if req is None:
                break
            if not isinstance(req, dict):
                continue
            method = req.get("method")
            rid = req.get("id")

            if self._is_grab(method, req.get("params")):
                ok = self._grab(client)
                if rid is not None:
                    # Reply in the exec-dispatch shape so the TUI renders the
                    # outcome as a visible line for both slash.exec and
                    # command.dispatch (a bare {ok} showed as "no output").
                    text = (
                        "You now control this session. Type to drive the agent."
                        if ok else
                        "This is a watch-only ticket; you cannot take control."
                    )
                    client.transport.write({
                        "jsonrpc": "2.0", "id": rid,
                        "result": {"type": "exec", "output": text},
                    })
                continue

            # Pin the joiner to the shared session: a request naming any other
            # session is refused, so a joiner cannot read or drive the host's
            # other sessions by guessing an id. The shared session is addressable
            # by its ephemeral id (events/submit) or its resume key (resume).
            params = req.get("params")
            req_sid = params.get("session_id") if isinstance(params, dict) else None
            if req_sid and req_sid != client.pinned_sid and req_sid != client.pinned_key:
                if rid is not None:
                    client.transport.write({
                        "jsonrpc": "2.0", "id": rid,
                        "error": {"code": 4031, "message": "not the shared session"},
                    })
                continue

            if not self._authorized(client, method):
                if rid is not None:
                    client.transport.write({
                        "jsonrpc": "2.0", "id": rid,
                        "error": {"code": 4030, "message": self._deny_reason(client, method)},
                    })
                continue

            try:
                resp = await self._loop.run_in_executor(None, server.dispatch, req, client.transport)
            except Exception:
                logger.exception("share: dispatch crashed method=%s", method)
                if rid is not None:
                    client.transport.write({
                        "jsonrpc": "2.0", "id": rid,
                        "error": {"code": -32603, "message": "internal error"},
                    })
                continue
            if resp is not None:
                client.transport.write(resp)

    # -- auth + control ----------------------------------------------------

    def _lookup_token(self, token: str) -> tuple[Optional[_SharedSession], Optional[str]]:
        """Resolve a token to its ``(shared_session, role)``, else ``(None, None)``.

        Compared in constant time against every registered token so a wrong
        token cannot be distinguished from one for a no-longer-shared session by
        timing. The loop does not break early for the same reason.
        """
        if not token:
            return None, None
        with self._reg_lock:
            match: Optional[tuple[_SharedSession, str]] = None
            for registered, entry in self._by_token.items():
                if secrets.compare_digest(token, registered):
                    match = entry
            return match if match is not None else (None, None)

    @staticmethod
    def _is_grab(method, params) -> bool:
        if method == "command.dispatch" and isinstance(params, dict):
            return (params.get("name") or "").lstrip("/").lower() == "grab"
        if method == "slash.exec" and isinstance(params, dict):
            return (params.get("command") or "").strip().lstrip("/").lower() == "grab"
        return False

    def _authorized(self, client: _Client, method: Optional[str]) -> bool:
        if not method:
            return False
        if method in _READ_ONLY_METHODS:
            return True
        # Mutating: only the client controlling THIS session may drive the agent.
        return client.shared.controller is client

    def _deny_reason(self, client: _Client, method: Optional[str]) -> str:
        if client.role != ROLE_CONTROL:
            return "watch-only: this ticket cannot control the session"
        return f"{self._holder_name(client.shared)} has control. Type /grab to take it."

    def _grab(self, client: _Client) -> bool:
        if client.role != ROLE_CONTROL:
            self._notice(client.shared, "watch-only ticket cannot take control", only=client)
            return False
        self._set_controller(client.shared, client)
        return True

    # -- public control API (consulted by the gateway's prompt.submit) --------

    def grab_host(self, key: str) -> None:
        """Return control of session ``key`` to the host (the local pane)."""
        with self._reg_lock:
            shared = self._shared.get(key)
        if shared is not None:
            self._set_controller(shared, None)

    def control_denied(self, transport, key: str) -> Optional[str]:
        """A refusal message if ``transport`` may not drive session ``key``.

        Returns None when the session is not shared (the host drives it freely)
        or when ``transport`` is the session's current controller. The host
        drives through the process stdio transport (the fan-out); a joiner drives
        through its own client transport.
        """
        if not key:
            return None
        with self._reg_lock:
            shared = self._shared.get(key)
        if shared is None:
            return None
        ctrl = shared.controller
        if ctrl is None:
            if transport is server._stdio_transport:
                return None
        elif transport is ctrl.transport:
            return None
        return f"{self._holder_name(shared)} has control. Type /grab to take it."

    def _set_controller(self, shared: _SharedSession, client: Optional[_Client]) -> None:
        if shared.controller is client:
            return
        shared.controller = client
        holder = self._holder_name(shared)
        # Announce to this session's participants so all panes agree on who
        # drives. session_id scopes it so only this session's panes see it.
        self._broadcast(shared, f"{holder} now has control.")
        # Structured counterpart of the toast: lets panes (the TUI status bar, the
        # dioxus client) track the holder precisely rather than parsing the text.
        try:
            server._emit("control.update", shared.sid, {"controller": holder})
        except Exception:
            pass

    def _holder_name(self, shared: _SharedSession) -> str:
        return shared.controller.name if shared.controller is not None else "host"

    def _notice(self, shared: _SharedSession, text: str,
                only: Optional[_Client] = None) -> None:
        if only is not None:
            frame = {"jsonrpc": "2.0", "method": "event", "params": {
                "type": "notification.show",
                "payload": {"text": f"[share] {text}", "kind": "ttl",
                            "ttl_ms": 6000, "level": "info"},
            }}
            try:
                only.transport.write(frame)
            except Exception:
                pass
            return
        self._broadcast(shared, text)

    def _broadcast(self, shared: _SharedSession, text: str) -> None:
        # notification.show is the gateway's toast channel. session_id scopes the
        # toast to this session's panes (host pane + this session's joiners); the
        # per-session frame filter drops it for joiners on other sessions.
        # 'ttl' self-expires. write_json reaches the host primary and the joiners.
        try:
            server.write_json({"jsonrpc": "2.0", "method": "event", "params": {
                "type": "notification.show",
                "session_id": shared.sid,
                "payload": {"text": f"[share] {text}", "kind": "ttl",
                            "ttl_ms": 6000, "level": "info"},
            }})
        except Exception:
            pass

    async def _raw_write(self, send, obj: dict) -> None:
        try:
            await send.write_all((json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8"))
        except Exception:
            pass


class JoinError(ShareError):
    """A join (bridge) operation failed."""


def start_join_bridge(ticket: str, name: str = "guest") -> tuple[int, Optional[str]]:
    """Connect to a shared host over iroh and expose it as a local WebSocket.

    Returns ``(port, session_id)``. The caller points the Ink TUI at
    ``ws://127.0.0.1:<port>`` (HERMES_TUI_GATEWAY_URL) and resumes
    ``session_id`` (HERMES_TUI_RESUME) so the real TUI renders the host's live
    session. The bridge runs on a daemon thread for the lifetime of the process:
    it pumps newline-JSON frames between the single local WS client (the TUI)
    and the iroh connection, buffering host frames that arrive before the TUI
    connects.
    """
    iroh = _require_iroh()
    base, token = _split_ticket(ticket)
    result: dict = {}
    ready = threading.Event()

    def _run() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(_bridge_setup(iroh, base, token, name, result, loop))
        except BaseException as exc:  # noqa: BLE001 - surfaced via the caller
            result["error"] = exc
            ready.set()
            loop.close()
            return
        ready.set()
        try:
            loop.run_forever()
        finally:
            # Drain cancelled iroh/WS tasks so their continuations settle before
            # the loop closes (avoids "Event loop is closed" at teardown).
            try:
                pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
                for task in pending:
                    task.cancel()
                if pending:
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass
            loop.close()

    threading.Thread(target=_run, name="hermes-join-bridge", daemon=True).start()
    ready.wait(timeout=40)
    if result.get("error") is not None:
        raise JoinError(f"could not join: {result['error']}") from result["error"]
    if "port" not in result:
        raise JoinError("join bridge did not start in time")
    return result["port"], result.get("session_id")


async def _bridge_setup(iroh, base, token, name, result, loop) -> None:
    import websockets

    endpoint = await iroh.Endpoint.bind(
        iroh.EndpointOptions(preset=iroh.preset_n0(), alpns=[ALPN])
    )
    conn = await endpoint.connect(_connect_addr(iroh, base), ALPN)
    bi = await conn.open_bi()
    recv, send = bi.recv(), bi.send()
    reader = _LineReader(recv)

    # JSON-RPC handshake: send a `hello` request, read the welcome as its result.
    hello = {"jsonrpc": "2.0", "id": 0, "method": "hello",
             "params": {"token": token, "name": name}}
    await send.write_all((json.dumps(hello) + "\n").encode("utf-8"))
    welcome = await reader.next()
    if not isinstance(welcome, dict) or "error" in welcome:
        message = "host refused the connection"
        if isinstance(welcome, dict):
            message = (welcome.get("error") or {}).get("message", message)
        raise JoinError(message)
    result["session_id"] = (welcome.get("result") or {}).get("session_id")

    state: dict = {"ws": None, "buffer": []}

    async def ws_handler(ws, *_args):
        state["ws"] = ws
        for line in state["buffer"]:
            await ws.send(line)
        state["buffer"].clear()
        try:
            async for message in ws:
                await send.write_all((message.strip() + "\n").encode("utf-8"))
        except Exception:
            pass
        finally:
            state["ws"] = None
            loop.stop()  # the TUI closed; tear the bridge down

    ws_server = await websockets.serve(ws_handler, "127.0.0.1", 0)
    result["port"] = ws_server.sockets[0].getsockname()[1]

    # Keep the iroh endpoint, connection, streams, and WS server referenced for
    # the life of the bridge thread. Without this they are local to this
    # coroutine, which returns immediately, and would be garbage-collected,
    # closing the iroh connection and the WS listener out from under the pump.
    result["_keepalive"] = (endpoint, conn, send, recv, ws_server)

    async def _pump() -> None:
        while True:
            frame = await reader.next()
            if frame is None:
                if state["ws"] is not None:
                    try:
                        await state["ws"].close()
                    except Exception:
                        pass
                loop.stop()
                return
            line = json.dumps(frame, ensure_ascii=False)
            if state["ws"] is not None:
                try:
                    await state["ws"].send(line)
                    continue
                except Exception:
                    pass
            state["buffer"].append(line)
            if len(state["buffer"]) > _MAX_BRIDGE_BUFFER:
                state["buffer"].pop(0)  # drop oldest if the TUI is slow to attach

    async def _connect_deadline() -> None:
        await asyncio.sleep(_BRIDGE_CONNECT_TIMEOUT)
        if state["ws"] is None:
            logger.debug("join bridge: TUI never connected; tearing down")
            loop.stop()

    loop.create_task(_pump())
    loop.create_task(_connect_deadline())


def _sanitize(text: str) -> str:
    """Drop control characters from peer-supplied display text."""
    return "".join(ch for ch in text if ch >= " " and ch != "\x7f")


def _frame_for_sid(obj: dict, attached_sid: Optional[str]) -> bool:
    """Whether a frame should reach a joiner attached to ``attached_sid``.

    Responses (an id, no session_id) and gateway-level events always pass.
    Session events pass only when they belong to the attached session, so a
    joiner never sees the host's other sessions.
    """
    if not isinstance(obj, dict):
        return True
    params = obj.get("params")
    if not isinstance(params, dict):
        return True
    sid = params.get("session_id")
    if not sid or attached_sid is None:
        return True
    return sid == attached_sid
