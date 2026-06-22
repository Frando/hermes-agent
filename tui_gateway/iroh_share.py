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
import json
import logging
import secrets
import threading
from typing import Optional

from tui_gateway import server

logger = logging.getLogger(__name__)

ALPN = b"hermes/tui-share/1"
_TICKET_SEP = "~"
_READ_CHUNK = 64 * 1024
_MAX_LINE_BYTES = 1024 * 1024
_MAX_CLIENTS = 32
_HELLO_TIMEOUT = 10.0
_ONLINE_TIMEOUT = 8.0
_MAX_QUEUED = 2000

ROLE_WATCH = "watch"
ROLE_CONTROL = "control"

# Methods a watch joiner may call: observation and attachment only. Everything
# not in this set (including any method added later) is treated as mutating, so
# it is denied to watchers and gated behind control for controllers. Failing
# closed is deliberate: a new mutating method must never silently become
# reachable by a watch ticket.
_READ_ONLY_METHODS = frozenset({
    "agents.list", "billing.state", "billing.charge_status", "commands.catalog",
    "command.resolve", "complete.path", "complete.slash", "config.get",
    "config.show", "credits.view", "delegation.status", "handoff.state",
    "insights.get", "model.options", "plugins.list", "process.list",
    "rollback.list", "rollback.diff", "session.active_list", "session.history",
    "session.list", "session.most_recent", "session.status", "session.usage",
    "session.resume", "setup.status", "setup.runtime_check", "spawn_tree.list",
    "spawn_tree.load", "toolsets.list", "tools.list", "tools.show",
    "paste.collapse", "input.detect_drop", "preview.restart",
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
            "Session sharing needs the 'iroh' package. "
            "Install it with: pip install 'hermes-agent[share]'"
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
        if self._queue.qsize() >= _MAX_QUEUED:
            try:
                self._loop.call_soon_threadsafe(self._queue.get_nowait)
            except Exception:
                pass
        try:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, obj)
        except RuntimeError:
            return False
        return True

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
    __slots__ = ("cid", "name", "role", "transport")

    def __init__(self, cid: str, name: str, role: str, transport: _ClientTransport):
        self.cid = cid
        self.name = name
        self.role = role
        self.transport = transport


class IrohShareHost:
    """Accept iroh joiners and bridge them into the running gateway."""

    def __init__(self):
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._endpoint = None
        self._clients: dict[str, _Client] = {}
        self._controller_cid: Optional[str] = None
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
        base = str(iroh.EndpointTicket.from_addr(self._endpoint.addr()))
        self._tickets = (
            f"{base}{_TICKET_SEP}{self._watch_token}",
            f"{base}{_TICKET_SEP}{self._control_token}",
        )
        self._loop.create_task(self._accept_loop())

    def _begin_shutdown(self) -> None:
        self._loop.create_task(self._shutdown_and_stop())

    async def _shutdown_and_stop(self) -> None:
        try:
            if self._endpoint is not None:
                await self._endpoint.close()
        except Exception:
            pass
        self._loop.stop()

    # -- accept + per-connection ------------------------------------------

    async def _accept_loop(self) -> None:
        while not self._closing:
            try:
                incoming = await self._endpoint.accept_next()
            except Exception:
                if self._closing:
                    break
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

            sid = server.active_shared_session_id()
            transport = _ClientTransport(send, self._loop, sid)
            client = _Client(secrets.token_hex(4), name, role, transport)
            self._clients[client.cid] = client
            server.attach_shared_transport(transport)

            # Welcome carries the session id so the remote TUI resumes the
            # host's live session instead of creating its own.
            await self._raw_write(send, {
                "type": "welcome", "role": role, "session_id": sid,
                "controller": self._controller_name(),
            })
            transport.write({
                "jsonrpc": "2.0", "method": "event",
                "params": {"type": "gateway.ready", "payload": {"skin": server.resolve_skin()}},
            })
            self._notice(f"{name} joined ({role})")
            await self._read_loop(client, reader)
        finally:
            if client is not None:
                self._clients.pop(client.cid, None)
                if self._controller_cid == client.cid:
                    self._controller_cid = None
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
                self._grab(client)
                if rid is not None:
                    client.transport.write({"jsonrpc": "2.0", "id": rid, "result": {"ok": True}})
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
        return client.role == ROLE_CONTROL and self._controller_cid == client.cid

    def _deny_reason(self, client: _Client, method: Optional[str]) -> str:
        if client.role != ROLE_CONTROL:
            return "watch-only: this ticket cannot control the session"
        return "not in control: type /grab to take control"

    def _grab(self, client: _Client) -> None:
        if client.role != ROLE_CONTROL:
            self._notice("watch-only ticket cannot take control", only=client)
            return
        self._controller_cid = client.cid
        self._notice(f"control held by {client.name}")

    def _controller_name(self) -> str:
        c = self._clients.get(self._controller_cid or "")
        return c.name if c else "host"

    def _notice(self, text: str, only: Optional[_Client] = None) -> None:
        frame = {"jsonrpc": "2.0", "method": "event",
                 "params": {"type": "notice", "payload": {"text": f"[share] {text}"}}}
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
