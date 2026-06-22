"""End-to-end: an iroh joiner attaches to a live gateway session.

Validates the core protocol plumbing without a model turn: a control joiner
connects over iroh, gets a welcome carrying the host session id, issues a
read-only request and receives the response back over iroh, and receives a
session event fanned out from the gateway. Skips when iroh is absent.
"""

import asyncio
import json

import pytest

iroh = pytest.importorskip("iroh")

from tui_gateway import iroh_share as sh  # noqa: E402
from tui_gateway import server  # noqa: E402


async def _raw_client(ticket, requests):
    base, token = sh._split_ticket(ticket)
    ep = await iroh.Endpoint.bind(
        iroh.EndpointOptions(preset=iroh.preset_n0_disable_relay(), alpns=[sh.ALPN])
    )
    conn = await ep.connect(iroh.EndpointTicket.from_string(base).endpoint_addr(), sh.ALPN)
    bi = await conn.open_bi()
    recv, send = bi.recv(), bi.send()
    reader = sh._LineReader(recv)
    frames = []

    async def drain():
        while True:
            msg = await reader.next()
            if msg is None:
                break
            frames.append(msg)

    task = asyncio.create_task(drain())

    async def write(obj):
        await send.write_all((json.dumps(obj) + "\n").encode("utf-8"))

    await write({"type": "hello", "token": token, "name": "tester"})
    await asyncio.sleep(0.4)
    for req in requests:
        await write(req)
        await asyncio.sleep(0.3)
    # Fan-out check: emit a session event from the gateway side; it must reach us.
    sid = server.active_shared_session_id()
    server.write_json({
        "jsonrpc": "2.0", "method": "event",
        "params": {"type": "test.fanout", "session_id": sid, "payload": {"k": 1}},
    })
    await asyncio.sleep(0.5)
    task.cancel()
    await ep.close()
    return frames


def test_iroh_joiner_attaches_and_receives_fanout(monkeypatch):
    monkeypatch.setattr(iroh, "preset_n0", iroh.preset_n0_disable_relay)
    # enable_sharing_fanout swaps the module-global stdio transport; record it
    # so monkeypatch restores it and the swap does not leak into other tests.
    monkeypatch.setattr(server, "_stdio_transport", server._stdio_transport)
    server.enable_sharing_fanout()
    # Create a host session bound to the shared fan-out (transport=None binds
    # the global stdio transport, which enable_sharing_fanout made a fan-out).
    server.dispatch({"jsonrpc": "2.0", "id": 1, "method": "session.create", "params": {}})
    sid = server.active_shared_session_id()
    assert sid, "expected a live host session"

    host = sh.IrohShareHost()
    _watch, control = host.start(online_timeout=2)
    try:
        frames = asyncio.run(_raw_client(
            control,
            [{"jsonrpc": "2.0", "id": 10, "method": "commands.catalog", "params": {}}],
        ))
    finally:
        host.stop()

    welcome = next((f for f in frames if f.get("type") == "welcome"), None)
    assert welcome is not None
    assert welcome["role"] == sh.ROLE_CONTROL
    assert welcome["session_id"] == sid

    # The read-only request got a response back over iroh.
    resp = next((f for f in frames if f.get("id") == 10), None)
    assert resp is not None, f"no response to commands.catalog; frames={[f.get('type') or f.get('id') for f in frames]}"

    # The session event fanned out to the joiner.
    fanned = next(
        (f for f in frames
         if f.get("method") == "event"
         and f.get("params", {}).get("type") == "test.fanout"),
        None,
    )
    assert fanned is not None, "session event did not reach the joiner"
    assert fanned["params"]["session_id"] == sid
