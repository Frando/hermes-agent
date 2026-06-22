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


def test_share_tickets_reports_not_sharing(monkeypatch):
    monkeypatch.setattr(server, "_iroh_share_host", None)
    resp = server.dispatch({"jsonrpc": "2.0", "id": 1, "method": "share.tickets", "params": {}})
    assert resp["result"] == {"sharing": False}


def test_share_tickets_reports_current_tickets(monkeypatch):
    class _FakeHost:
        tickets = ("WATCH-TICKET", "CONTROL-TICKET")

    monkeypatch.setattr(server, "_iroh_share_host", _FakeHost())
    resp = server.dispatch({"jsonrpc": "2.0", "id": 2, "method": "share.tickets", "params": {}})
    assert resp["result"] == {
        "sharing": True, "watch": "WATCH-TICKET", "control": "CONTROL-TICKET",
    }


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
