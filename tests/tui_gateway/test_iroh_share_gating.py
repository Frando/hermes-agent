"""Auth, grab, and frame-filtering logic for the iroh share acceptor.

These exercise the security-critical gating without needing iroh or a live
gateway: per-session token assignment, the read-only allow-list, grab handling,
and the per-session frame filter. Sharing is per-session: each shared session
carries its own tokens and its own controller.
"""

from tui_gateway import iroh_share as sh


def _shared(key="k1", sid="s1", watch="watchtok", control="ctrltok"):
    return sh._SharedSession(
        key, sid, watch, control,
        f"id/{watch}", f"id/{control}",
    )


def _host_with(shared):
    """A host with one registered shared session (no endpoint bound)."""
    h = sh.IrohShareHost()
    h._shared[shared.key] = shared
    h._by_token[shared.watch_token] = (shared, sh.ROLE_WATCH)
    h._by_token[shared.control_token] = (shared, sh.ROLE_CONTROL)
    return h


def _client(role, shared, cid="c1", name="x"):
    return sh._Client(cid, name, role, transport=None, shared=shared)


def test_split_ticket():
    base, tok = sh._split_ticket("deadbeef0123/secret")
    assert base == "deadbeef0123"
    assert tok == "secret"


def test_split_ticket_uses_last_separator():
    base, tok = sh._split_ticket("a/b/tok")
    assert (base, tok) == ("a/b", "tok")


def test_sanitize_strips_controls():
    assert sh._sanitize("a\x07b\x7f\r\n") == "ab"


def test_lookup_token_resolves_session_and_role():
    shared = _shared()
    h = _host_with(shared)
    s, role = h._lookup_token("ctrltok")
    assert s is shared and role == sh.ROLE_CONTROL
    s, role = h._lookup_token("watchtok")
    assert s is shared and role == sh.ROLE_WATCH
    assert h._lookup_token("") == (None, None)
    assert h._lookup_token("nope") == (None, None)


def test_lookup_token_isolates_sessions():
    # Two shared sessions, each with its own tokens, resolve independently.
    a = _shared(key="ka", sid="sa", watch="wa", control="ca")
    b = _shared(key="kb", sid="sb", watch="wb", control="cb")
    h = sh.IrohShareHost()
    for s in (a, b):
        h._shared[s.key] = s
        h._by_token[s.watch_token] = (s, sh.ROLE_WATCH)
        h._by_token[s.control_token] = (s, sh.ROLE_CONTROL)
    assert h._lookup_token("ca")[0] is a
    assert h._lookup_token("wb")[0] is b


def test_unshared_token_is_rejected():
    # Once a session is unshared its tokens resolve to nothing, so a holder of an
    # old ticket can no longer connect.
    shared = _shared()
    h = _host_with(shared)
    assert h.unshare_session("k1") is True
    assert h._lookup_token("ctrltok") == (None, None)
    assert h.unshare_session("k1") is False  # already gone


def test_is_grab():
    assert sh.IrohShareHost._is_grab("command.dispatch", {"name": "grab"})
    assert sh.IrohShareHost._is_grab("command.dispatch", {"name": "/grab"})
    assert sh.IrohShareHost._is_grab("slash.exec", {"command": "/grab"})
    assert not sh.IrohShareHost._is_grab("prompt.submit", {"text": "hi"})
    assert not sh.IrohShareHost._is_grab("command.dispatch", {"name": "model"})


def test_watch_role_blocked_from_mutating():
    h = _host_with(_shared())
    watcher = _client(sh.ROLE_WATCH, h._shared["k1"])
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
    h = _host_with(_shared())
    watcher = _client(sh.ROLE_WATCH, h._shared["k1"])
    assert h._authorized(watcher, "session.list") is False
    assert h._authorized(watcher, "session.active_list") is False
    assert h._authorized(watcher, "session.most_recent") is False


def test_unknown_method_is_treated_as_mutating():
    h = _host_with(_shared())
    shared = h._shared["k1"]
    watcher = _client(sh.ROLE_WATCH, shared)
    controller = _client(sh.ROLE_CONTROL, shared)
    assert h._authorized(watcher, "some.future_method") is False
    assert h._authorized(controller, "some.future_method") is False


def test_control_requires_holding_grab():
    h = _host_with(_shared())
    controller = _client(sh.ROLE_CONTROL, h._shared["k1"], cid="c1")
    assert h._authorized(controller, "commands.catalog") is True
    assert h._authorized(controller, "prompt.submit") is False
    h._grab(controller)
    assert h._shared["k1"].controller is controller
    assert h._authorized(controller, "prompt.submit") is True


def test_grab_is_refused_for_watch():
    h = _host_with(_shared())
    watcher = _client(sh.ROLE_WATCH, h._shared["k1"], cid="w1")
    h._grab(watcher)
    assert h._shared["k1"].controller is None  # watch can never take control


def test_second_controller_steals_grab():
    h = _host_with(_shared())
    shared = h._shared["k1"]
    a = _client(sh.ROLE_CONTROL, shared, cid="a")
    b = _client(sh.ROLE_CONTROL, shared, cid="b")
    h._grab(a)
    assert h._authorized(a, "prompt.submit") is True
    h._grab(b)
    assert shared.controller is b
    assert h._authorized(a, "prompt.submit") is False  # a lost control
    assert h._authorized(b, "prompt.submit") is True


def test_control_is_per_session():
    # Grabbing control of one session does not grant control of another.
    a = _shared(key="ka", sid="sa", watch="wa", control="ca")
    b = _shared(key="kb", sid="sb", watch="wb", control="cb")
    h = sh.IrohShareHost()
    for s in (a, b):
        h._shared[s.key] = s
    ca = _client(sh.ROLE_CONTROL, a, cid="ca")
    h._grab(ca)
    assert a.controller is ca
    assert b.controller is None
    # The controller of A is not authorized to drive B.
    cb_view = _client(sh.ROLE_CONTROL, b, cid="ca")
    assert h._authorized(cb_view, "prompt.submit") is False


def _started_host():
    # A host with the endpoint pretend-bound, so share_session runs without
    # touching iroh (ensure_started returns immediately when _started is set).
    h = sh.IrohShareHost()
    h._started = True
    h._endpoint_b32 = "endpointbase"
    return h


def test_share_session_is_idempotent():
    # Sharing the same session twice returns the SAME tickets, so a repeated
    # /share reprints the existing join commands rather than re-minting tokens.
    h = _started_host()
    first = h.share_session("sid1", "key1")
    second = h.share_session("sid1", "key1")
    assert first == second
    watch, control = first
    assert watch != control
    assert h.shared_count == 1
    assert h._lookup_token(watch.rsplit("/", 1)[1]) == (h._shared["key1"], sh.ROLE_WATCH)
    assert h._lookup_token(control.rsplit("/", 1)[1]) == (h._shared["key1"], sh.ROLE_CONTROL)


def test_unshare_then_share_mints_new_tokens():
    h = _started_host()
    watch1, control1 = h.share_session("sid1", "key1")
    assert h.unshare_session("key1") is True
    assert h.unshare_session("key1") is False  # already unshared
    assert h.shared_count == 0
    # Old tokens stop resolving once unshared.
    assert h._lookup_token(watch1.rsplit("/", 1)[1]) == (None, None)
    # Re-sharing mints fresh tokens: a new share is a new grant.
    watch2, control2 = h.share_session("sid1", "key1")
    assert watch2 != watch1
    assert control2 != control1
    assert h._lookup_token(watch2.rsplit("/", 1)[1])[1] == sh.ROLE_WATCH


def test_multiple_sessions_share_one_endpoint():
    # Per-session tokens off one shared endpoint id.
    h = _started_host()
    w1, c1 = h.share_session("sidA", "keyA")
    w2, c2 = h.share_session("sidB", "keyB")
    assert h.shared_count == 2
    assert {t.rsplit("/", 1)[0] for t in (w1, c1, w2, c2)} == {"endpointbase"}
    assert h._lookup_token(w1.rsplit("/", 1)[1])[0].key == "keyA"
    assert h._lookup_token(c2.rsplit("/", 1)[1])[0].key == "keyB"


def test_host_is_default_controller_and_can_grab_back():
    # The stdio (fan-out) transport is the controller by default for a shared
    # session; a joiner grab transfers it; grab_host reclaims it.
    from tui_gateway import server

    h = _host_with(_shared())
    shared = h._shared["k1"]
    a = _client(sh.ROLE_CONTROL, shared, cid="a")

    class _T:
        def write(self, o):
            return True

    a.transport = _T()

    assert h.control_denied(server._stdio_transport, "k1") is None
    assert "has control" in (h.control_denied(a.transport, "k1") or "")

    h._grab(a)
    assert h.control_denied(a.transport, "k1") is None
    assert "has control" in (h.control_denied(server._stdio_transport, "k1") or "")

    h.grab_host("k1")
    assert h.control_denied(server._stdio_transport, "k1") is None
    assert "has control" in (h.control_denied(a.transport, "k1") or "")


def test_control_denied_is_noop_for_unshared_session():
    # When a session is not shared, no one is gated: the host drives freely.
    h = sh.IrohShareHost()
    from tui_gateway import server

    assert h.control_denied(server._stdio_transport, "not-shared") is None
    assert h.control_denied(object(), "not-shared") is None
    assert h.control_denied(object(), "") is None


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
