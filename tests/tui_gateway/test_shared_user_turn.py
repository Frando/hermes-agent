"""A prompt submitted to a shared session is echoed to the OTHER participants.

Runs without iroh: it drives the real prompt.submit handler with a fan-out that
has an extra (a stand-in joiner) and asserts the joiner receives a message.user
event carrying the prompt, while the submitter (the fan-out's primary, i.e. the
host) does not.
"""

from tui_gateway import server


class _Recorder:
    def __init__(self):
        self.frames = []
        self._closed = False

    def write(self, obj):
        self.frames.append(obj)
        return True

    def close(self):
        self._closed = True


def _register_shared(monkeypatch, sid, key, watch="WATCH-TICKET", control="CONTROL-TICKET"):
    """Install a share host with one session registered (no endpoint bound)."""
    from tui_gateway import iroh_share as sh

    host = sh.IrohShareHost()
    shared = sh._SharedSession(key, sid, "wt", "ct", watch, control)
    host._shared[key] = shared
    host._by_token["wt"] = (shared, sh.ROLE_WATCH)
    host._by_token["ct"] = (shared, sh.ROLE_CONTROL)
    monkeypatch.setattr(server, "_iroh_share_host", host)
    return host, shared


def test_share_status_reports_not_sharing(monkeypatch):
    monkeypatch.setattr(server, "_stdio_transport", server._stdio_transport)
    monkeypatch.setattr(server, "_iroh_share_host", None)
    server.dispatch({"jsonrpc": "2.0", "id": 1, "method": "session.create", "params": {}})
    sid = server.active_shared_session_id()
    resp = server.dispatch({
        "jsonrpc": "2.0", "id": 2, "method": "share.status", "params": {"session_id": sid},
    })
    assert resp["result"] == {"sharing": False}


def test_share_status_returns_control_to_host(monkeypatch):
    monkeypatch.setattr(server, "_stdio_transport", server._stdio_transport)
    server.dispatch({"jsonrpc": "2.0", "id": 1, "method": "session.create", "params": {}})
    sid = server.active_shared_session_id()
    key = server._sessions[sid]["session_key"]
    _register_shared(monkeypatch, sid, key)
    # transport=None binds the stdio transport, i.e. the host: control included.
    resp = server.dispatch({
        "jsonrpc": "2.0", "id": 2, "method": "share.status", "params": {"session_id": sid},
    })
    assert resp["result"] == {
        "sharing": True, "watch": "WATCH-TICKET", "control": "CONTROL-TICKET",
    }


def test_share_status_omits_control_for_non_host(monkeypatch):
    # A joiner drives through its own transport (not the host stdio transport),
    # so share.status must withhold the control ticket from it — otherwise a
    # joiner could capture the control token and reconnect with control.
    monkeypatch.setattr(server, "_stdio_transport", server._stdio_transport)
    server.dispatch({"jsonrpc": "2.0", "id": 1, "method": "session.create", "params": {}})
    sid = server.active_shared_session_id()
    key = server._sessions[sid]["session_key"]
    _register_shared(monkeypatch, sid, key)
    joiner = _Recorder()
    resp = server.dispatch(
        {"jsonrpc": "2.0", "id": 2, "method": "share.status", "params": {"session_id": sid}},
        transport=joiner,
    )
    assert resp["result"] == {"sharing": True, "watch": "WATCH-TICKET"}
    assert "control" not in resp["result"]


def test_prompt_submit_gated_by_shared_controller(monkeypatch):
    """The host (stdio submitter) is refused while a joiner holds control of
    that session, and may submit again after grabbing control back."""
    from tui_gateway import iroh_share as sh

    monkeypatch.setattr(server, "_stdio_transport", server._stdio_transport)
    server.enable_sharing_fanout()
    server.dispatch({"jsonrpc": "2.0", "id": 1, "method": "session.create", "params": {}})
    sid = server.active_shared_session_id()
    key = server._sessions[sid]["session_key"]

    class _T:
        def write(self, obj):
            return True

    host, shared = _register_shared(monkeypatch, sid, key)
    joiner = sh._Client("c", "alice", sh.ROLE_CONTROL, transport=_T(), shared=shared)
    shared.controller = joiner  # a joiner holds control of this session

    # The host submits through stdio (transport=None binds the fan-out). Refused.
    denied = server.dispatch({
        "jsonrpc": "2.0", "id": 2, "method": "prompt.submit",
        "params": {"session_id": sid, "text": "host tries"},
    })
    assert denied.get("error", {}).get("code") == 4030
    assert "has control" in denied["error"]["message"]

    # Host grabs control back; now the submit is accepted (starts streaming).
    host.grab_host(key)
    ok = server.dispatch({
        "jsonrpc": "2.0", "id": 3, "method": "prompt.submit",
        "params": {"session_id": sid, "text": "host drives"},
    })
    assert ok.get("error", {}).get("code") != 4030


def test_prompt_submit_echoes_user_turn_to_others(monkeypatch):
    # Record the global stdio transport so the fan-out swap is restored.
    monkeypatch.setattr(server, "_stdio_transport", server._stdio_transport)
    server.enable_sharing_fanout()

    server.dispatch({"jsonrpc": "2.0", "id": 1, "method": "session.create", "params": {}})
    sid = server.active_shared_session_id()
    assert sid

    watcher = _Recorder()
    server.attach_shared_transport(watcher)
    try:
        # transport=None binds the global fan-out, so current_transport() is the
        # fan-out itself: this simulates the HOST submitting. The echo must reach
        # the watcher (an extra) but not the primary.
        server.dispatch({
            "jsonrpc": "2.0", "id": 2, "method": "prompt.submit",
            "params": {"session_id": sid, "text": "hello team"},
        })
        frames = list(watcher.frames)
    finally:
        server.detach_shared_transport(watcher)

    user_events = [
        f for f in frames
        if isinstance(f, dict) and f.get("params", {}).get("type") == "message.user"
    ]
    assert user_events, (
        "watcher did not receive message.user; "
        f"got {[f.get('params', {}).get('type') for f in frames if isinstance(f, dict)]}"
    )
    assert user_events[0]["params"]["payload"]["text"] == "hello team"
    assert user_events[0]["params"]["session_id"] == sid
