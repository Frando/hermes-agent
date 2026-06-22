"""Auth, grab, and frame-filtering logic for the iroh share acceptor.

These exercise the security-critical gating without needing iroh or a live
gateway: role assignment from tokens, the read-only allow-list, grab handling,
and the per-session frame filter.
"""

from tui_gateway import iroh_share as sh


def _host():
    h = sh.IrohShareHost()
    h._watch_token = "watchtok"
    h._control_token = "ctrltok"
    return h


def _client(role, cid="c1", name="x"):
    return sh._Client(cid, name, role, transport=None, pinned_sid="s1")


def test_split_ticket():
    base, tok = sh._split_ticket("endpointabc~secret")
    assert base == "endpointabc"
    assert tok == "secret"


def test_split_ticket_uses_last_separator():
    base, tok = sh._split_ticket("a~b~tok")
    assert (base, tok) == ("a~b", "tok")


def test_sanitize_strips_controls():
    assert sh._sanitize("a\x07b\x7f\r\n") == "ab"


def test_role_for_token():
    h = _host()
    assert h._role_for_token("ctrltok") == sh.ROLE_CONTROL
    assert h._role_for_token("watchtok") == sh.ROLE_WATCH
    assert h._role_for_token("") is None
    assert h._role_for_token("nope") is None


def test_is_grab():
    assert sh.IrohShareHost._is_grab("command.dispatch", {"name": "grab"})
    assert sh.IrohShareHost._is_grab("command.dispatch", {"name": "/grab"})
    assert sh.IrohShareHost._is_grab("slash.exec", {"command": "/grab"})
    assert not sh.IrohShareHost._is_grab("prompt.submit", {"text": "hi"})
    assert not sh.IrohShareHost._is_grab("command.dispatch", {"name": "model"})


def test_watch_role_blocked_from_mutating():
    h = _host()
    watcher = _client(sh.ROLE_WATCH)
    assert h._authorized(watcher, "commands.catalog") is True  # read-only allowed
    assert h._authorized(watcher, "session.resume") is True    # attach allowed
    assert h._authorized(watcher, "prompt.submit") is False    # mutating denied
    assert h._authorized(watcher, "slash.exec") is False
    # Secret-leaking / agent-running / cross-session methods are NOT read-only.
    assert h._authorized(watcher, "config.get") is False
    assert h._authorized(watcher, "preview.restart") is False
    assert h._authorized(watcher, "spawn_tree.list") is False
    assert h._authorized(watcher, "complete.path") is False


def test_session_enumeration_denied_for_watchers():
    # A joiner must not be able to enumerate the host's other sessions.
    h = _host()
    watcher = _client(sh.ROLE_WATCH)
    assert h._authorized(watcher, "session.list") is False
    assert h._authorized(watcher, "session.active_list") is False
    assert h._authorized(watcher, "session.most_recent") is False


def test_unknown_method_is_treated_as_mutating():
    h = _host()
    watcher = _client(sh.ROLE_WATCH)
    controller = _client(sh.ROLE_CONTROL)
    # A method not in the read-only set fails closed for watch.
    assert h._authorized(watcher, "some.future_method") is False
    # And requires control for a controller.
    assert h._authorized(controller, "some.future_method") is False


def test_control_requires_holding_grab():
    h = _host()
    controller = _client(sh.ROLE_CONTROL, cid="c1")
    # Read-only always allowed.
    assert h._authorized(controller, "commands.catalog") is True
    # Mutating denied until this client holds control.
    assert h._authorized(controller, "prompt.submit") is False
    h._grab(controller)
    assert h._controller_cid == "c1"
    assert h._authorized(controller, "prompt.submit") is True


def test_grab_is_refused_for_watch():
    h = _host()
    watcher = _client(sh.ROLE_WATCH, cid="w1")
    h._grab(watcher)
    assert h._controller_cid is None  # watch can never take control


def test_second_controller_steals_grab():
    h = _host()
    a = _client(sh.ROLE_CONTROL, cid="a")
    b = _client(sh.ROLE_CONTROL, cid="b")
    h._grab(a)
    assert h._authorized(a, "prompt.submit") is True
    h._grab(b)
    assert h._controller_cid == "b"
    assert h._authorized(a, "prompt.submit") is False  # a lost control
    assert h._authorized(b, "prompt.submit") is True


def test_frame_filter_session_scoping():
    # Responses (no params) always pass.
    assert sh._frame_for_sid({"id": 1, "result": {}}, "s1") is True
    # Gateway-level events (no session_id) pass.
    assert sh._frame_for_sid({"method": "event", "params": {"type": "gateway.ready"}}, "s1") is True
    # Session events pass only for the attached session.
    matching = {"method": "event", "params": {"type": "tool.start", "session_id": "s1"}}
    other = {"method": "event", "params": {"type": "tool.start", "session_id": "s2"}}
    assert sh._frame_for_sid(matching, "s1") is True
    assert sh._frame_for_sid(other, "s1") is False
    # No attached sid yet -> everything passes.
    assert sh._frame_for_sid(other, None) is True
