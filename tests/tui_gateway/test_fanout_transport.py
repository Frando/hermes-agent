"""FanoutTransport and the session-transport bind helper.

These cover the one behavioral change the iroh sharing feature makes to the
gateway session model: a shared session fans events to several clients, and
binding a client to such a session adds it instead of stealing the stream.
"""

from tui_gateway.transport import FanoutTransport


class _RecordingTransport:
    """A minimal Transport that records frames and can simulate going dead."""

    def __init__(self, alive=True):
        self.frames = []
        self.alive = alive
        self.closed = False

    def write(self, obj):
        if not self.alive:
            return False
        self.frames.append(obj)
        return True

    def close(self):
        self.closed = True


def test_fanout_writes_to_primary_and_extras():
    primary = _RecordingTransport()
    a = _RecordingTransport()
    b = _RecordingTransport()
    fan = FanoutTransport(primary)
    fan.add(a)
    fan.add(b)
    assert fan.write({"n": 1}) is True
    assert primary.frames == [{"n": 1}]
    assert a.frames == [{"n": 1}]
    assert b.frames == [{"n": 1}]


def test_fanout_add_is_deduped_and_ignores_primary():
    primary = _RecordingTransport()
    a = _RecordingTransport()
    fan = FanoutTransport(primary)
    fan.add(a)
    fan.add(a)  # duplicate
    fan.add(primary)  # primary is never an extra
    fan.write({"n": 1})
    assert a.frames == [{"n": 1}]  # delivered once, not twice
    assert primary.frames == [{"n": 1}]


def test_fanout_return_value_tracks_primary_only():
    primary = _RecordingTransport(alive=False)
    a = _RecordingTransport()
    fan = FanoutTransport(primary)
    fan.add(a)
    # Primary dead -> write reports False even though the extra accepted it.
    assert fan.write({"n": 1}) is False
    assert a.frames == [{"n": 1}]


def test_fanout_prunes_dead_extras():
    primary = _RecordingTransport()
    dead = _RecordingTransport(alive=False)
    live = _RecordingTransport()
    fan = FanoutTransport(primary)
    fan.add(dead)
    fan.add(live)
    fan.write({"n": 1})  # dead returns False -> pruned
    fan.write({"n": 2})
    assert live.frames == [{"n": 1}, {"n": 2}]
    assert dead.frames == []  # never accepted anything


def test_fanout_remove():
    primary = _RecordingTransport()
    a = _RecordingTransport()
    fan = FanoutTransport(primary)
    fan.add(a)
    fan.remove(a)
    fan.write({"n": 1})
    assert a.frames == []
    assert primary.frames == [{"n": 1}]


def test_fanout_close_releases_extras_not_primary():
    primary = _RecordingTransport()
    a = _RecordingTransport()
    fan = FanoutTransport(primary)
    fan.add(a)
    fan.close()
    assert a.closed is True
    assert primary.closed is False  # primary outlives the fan-out


def test_bind_helper_replaces_for_plain_session():
    from tui_gateway.server import _bind_session_transport

    plain = _RecordingTransport()
    other = _RecordingTransport()
    session = {"transport": plain}
    _bind_session_transport(session, other)
    assert session["transport"] is other  # plain session: replace (legacy behavior)


def test_bind_helper_adds_for_shared_session():
    from tui_gateway.server import _bind_session_transport

    primary = _RecordingTransport()
    fan = FanoutTransport(primary)
    joiner = _RecordingTransport()
    session = {"transport": fan}
    _bind_session_transport(session, joiner)
    assert session["transport"] is fan  # slot unchanged: no steal
    fan.write({"n": 1})
    assert joiner.frames == [{"n": 1}]  # joiner attached as an extra


def test_fanout_add_rejects_self():
    # Adding the fan-out to itself must not happen (would recurse on write).
    primary = _RecordingTransport()
    fan = FanoutTransport(primary)
    fan.add(fan)
    fan.write({"n": 1})
    assert primary.frames == [{"n": 1}]  # delivered exactly once, no recursion


def test_bind_helper_self_bind_is_noop():
    # The host path binds the fan-out itself (current_transport() == the slot).
    from tui_gateway.server import _bind_session_transport

    primary = _RecordingTransport()
    fan = FanoutTransport(primary)
    session = {"transport": fan}
    _bind_session_transport(session, fan)  # must not add fan to its own members
    assert session["transport"] is fan
    fan.write({"n": 1})
    assert primary.frames == [{"n": 1}]  # exactly once


def test_bind_helper_ignores_none():
    from tui_gateway.server import _bind_session_transport

    plain = _RecordingTransport()
    session = {"transport": plain}
    _bind_session_transport(session, None)
    assert session["transport"] is plain
