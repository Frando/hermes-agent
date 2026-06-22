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
import secrets
import threading
from typing import Optional

from tui_gateway import server

logger = logging.getLogger(__name__)

ALPN = b"hermes/tui-share/1"
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


def _require_iroh():
    try:
        import iroh
    except ImportError as exc:
        raise ShareUnavailable(
            "Session sharing needs the 'iroh' package (a core dependency). "
            "Reinstall hermes-agent, or: pip install iroh"
        ) from exc
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


class _Client:
    __slots__ = ("cid", "name", "role", "transport", "pinned_sid", "pinned_key")

    def __init__(self, cid: str, name: str, role: str, transport: _ClientTransport,
                 pinned_sid: Optional[str], pinned_key: Optional[str]):
        self.cid = cid
        self.name = name
        self.role = role
        self.transport = transport
        # The joiner addresses the shared session two ways: the ephemeral id on
        # events and prompt.submit, and the resume key on session.resume. Both
        # are allowed; anything else is a different session and refused.
        self.pinned_sid = pinned_sid
        self.pinned_key = pinned_key


class IrohShareHost:
    """Accept iroh joiners and bridge them into the running gateway."""

    def __init__(self):
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._endpoint = None
        self._clients: dict[str, _Client] = {}
        # The single controller across ALL participants. None means the host
        # (the local `hermes share` pane) holds control; a _Client means that
        # joiner does. Only one party can drive the agent at a time.
        self._controller: Optional[_Client] = None
        self._watch_token = ""
        self._control_token = ""
        self._tickets: Optional[tuple[str, str]] = None
        self._ready = threading.Event()
        self._error: Optional[BaseException] = None
        self._closing = False

    # -- lifecycle ---------------------------------------------------------

    def start(self, online_timeout: float = _ONLINE_TIMEOUT) -> tuple[str, str]:
        """Bind the endpoint; return (watch_ticket, control_ticket)."""
        if self._tickets is not None:
            return self._tickets
        self._watch_token = secrets.token_urlsafe(12)
        self._control_token = secrets.token_urlsafe(12)
        self._thread = threading.Thread(
            target=self._run, args=(online_timeout,), name="hermes-tui-share", daemon=True
        )
        self._thread.start()
        self._ready.wait(timeout=online_timeout + 15)
        if self._error is not None:
            raise ShareError(f"failed to start sharing: {self._error}") from self._error
        if self._tickets is None:
            raise ShareError("sharing endpoint did not start in time")
        return self._tickets

    @property
    def tickets(self) -> Optional[tuple[str, str]]:
        """The (watch, control) tickets once bound, else None."""
        return self._tickets

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

    def _run(self, online_timeout: float) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._bind(online_timeout))
        except BaseException as exc:  # noqa: BLE001 - surfaced via start()
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
        base = _encode_id(self._endpoint.id())
        self._tickets = (
            f"{base}{_TICKET_SEP}{self._watch_token}",
            f"{base}{_TICKET_SEP}{self._control_token}",
        )
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
            if not hello or hello.get("type") != "hello":
                await self._raw_write(send, {"type": "error", "message": "expected hello"})
                return
            role = self._role_for_token(hello.get("token") or "")
            if role is None:
                await self._raw_write(send, {"type": "error", "message": "invalid ticket"})
                return
            if len(self._clients) >= _MAX_CLIENTS:
                await self._raw_write(send, {"type": "error", "message": "session full"})
                return
            name = _sanitize((hello.get("name") or "guest")).strip()[:40] or "guest"

            handle = server.shared_session_handle()
            if handle is None:
                # No live session to attach to. Refuse rather than create a
                # client with no pinned session, which would defeat the
                # per-session frame filter.
                await self._raw_write(send, {
                    "type": "error", "message": "host has no shareable session yet",
                })
                return
            sid, resume_key = handle

            # Events and prompt.submit use the ephemeral id, so the event filter
            # and submit pin track it; the joiner resumes by the resume key.
            transport = _ClientTransport(send, self._loop, sid)
            client = _Client(
                secrets.token_hex(4), name, role, transport,
                pinned_sid=sid, pinned_key=resume_key,
            )

            # Send the welcome and gateway.ready FIRST, then attach to the
            # fan-out. Attaching last guarantees the joiner cannot receive a
            # session event ahead of gateway.ready. The welcome carries the
            # RESUME KEY so the remote TUI's session.resume reattaches to this
            # exact live session (resume is keyed by the persistent key, not the
            # ephemeral id) and gets the ephemeral id back to drive it with.
            await self._raw_write(send, {
                "type": "welcome", "role": role, "session_id": resume_key,
                "controller": self._controller_name(),
            })
            await self._raw_write(send, {
                "jsonrpc": "2.0", "method": "event",
                "params": {"type": "gateway.ready", "payload": {"skin": server.resolve_skin()}},
            })
            self._clients[client.cid] = client
            server.attach_shared_transport(transport)
            self._notice(f"{name} joined ({role})")
            await self._read_loop(client, reader)
        finally:
            if client is not None:
                self._clients.pop(client.cid, None)
                if self._controller is client:
                    # The controller left; control returns to the host.
                    self._set_controller(None)
                self._notice(f"{client.name} left")
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

    def _role_for_token(self, token: str) -> Optional[str]:
        if token and secrets.compare_digest(token, self._control_token):
            return ROLE_CONTROL
        if token and secrets.compare_digest(token, self._watch_token):
            return ROLE_WATCH
        return None

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
        # Mutating: only the controlling client may drive the agent.
        return self._controller is client

    def _deny_reason(self, client: _Client, method: Optional[str]) -> str:
        if client.role != ROLE_CONTROL:
            return "watch-only: this ticket cannot control the session"
        return f"{self._controller_name()} has control. Type /grab to take it."

    def _grab(self, client: _Client) -> bool:
        if client.role != ROLE_CONTROL:
            self._notice("watch-only ticket cannot take control", only=client)
            return False
        self._set_controller(client)
        return True

    # -- public control API (consulted by the gateway's prompt.submit) --------

    def grab_host(self) -> None:
        """Return control to the host (the local ``hermes share`` pane)."""
        self._set_controller(None)

    def is_controller(self, transport) -> bool:
        """Whether ``transport`` belongs to the current controller.

        The host drives through the process stdio transport (the fan-out); a
        joiner drives through its own client transport.
        """
        ctrl = self._controller
        if ctrl is None:
            return transport is server._stdio_transport
        return transport is ctrl.transport

    def control_denied(self, transport) -> Optional[str]:
        """A refusal message if ``transport`` is not the controller, else None."""
        if self.is_controller(transport):
            return None
        return f"{self._controller_name()} has control. Type /grab to take it."

    def _set_controller(self, client: Optional[_Client]) -> None:
        if self._controller is client:
            return
        self._controller = client
        # Announce to everyone (host + joiners) so all panes agree on who drives.
        holder = self._controller_name()
        try:
            server.write_json({"jsonrpc": "2.0", "method": "event", "params": {
                "type": "notification.show",
                "payload": {"text": f"[share] {holder} now has control.",
                            "kind": "ttl", "ttl_ms": 6000, "level": "info"},
            }})
        except Exception:
            pass

    def _controller_name(self) -> str:
        return self._controller.name if self._controller is not None else "host"

    def _notice(self, text: str, only: Optional[_Client] = None) -> None:
        # notification.show is the gateway's toast channel; no session_id so the
        # TUI does not drop it as belonging to another session. 'ttl' self-expires.
        frame = {"jsonrpc": "2.0", "method": "event", "params": {
            "type": "notification.show",
            "payload": {"text": f"[share] {text}", "kind": "ttl", "ttl_ms": 6000, "level": "info"},
        }}
        targets = [only] if only is not None else list(self._clients.values())
        for c in targets:
            try:
                c.transport.write(frame)
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

    await send.write_all(
        (json.dumps({"type": "hello", "token": token, "name": name}) + "\n").encode("utf-8")
    )
    welcome = await reader.next()
    if not welcome or welcome.get("type") == "error":
        raise JoinError((welcome or {}).get("message", "host refused the connection"))
    result["session_id"] = welcome.get("session_id")

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
