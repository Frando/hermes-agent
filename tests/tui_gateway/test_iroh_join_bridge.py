"""End-to-end: a WebSocket client through the join bridge to a live host.

Stands in for the Ink TUI with a raw WebSocket client and drives the full
chain: WS client -> local bridge -> iroh -> host acceptor -> gateway dispatch.
Confirms gateway.ready reaches the client and a read-only request is answered
back through the whole path. Skips without iroh or websockets.
"""

import asyncio
import json

import pytest

iroh = pytest.importorskip("iroh")
pytest.importorskip("websockets")

import websockets  # noqa: E402

from tui_gateway import iroh_share as sh  # noqa: E402
from tui_gateway import server  # noqa: E402


async def _ws_client(port, requests):
    frames = []
    async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
        async def drain():
            try:
                async for msg in ws:
                    frames.append(json.loads(msg))
            except Exception:
                pass

        task = asyncio.create_task(drain())
        await asyncio.sleep(0.4)
        for req in requests:
            await ws.send(json.dumps(req))
            await asyncio.sleep(0.4)
        await asyncio.sleep(0.4)
        task.cancel()
    return frames


def test_join_bridge_end_to_end(monkeypatch):
    monkeypatch.setattr(iroh, "preset_n0", iroh.preset_n0_disable_relay)
    # Record the module-global stdio transport so monkeypatch restores it; the
    # fan-out swap must not leak into other tests in the same process.
    monkeypatch.setattr(server, "_stdio_transport", server._stdio_transport)
    server.enable_sharing_fanout()
    server.dispatch({"jsonrpc": "2.0", "id": 1, "method": "session.create", "params": {}})
    sid = server.active_shared_session_id()
    assert sid

    host = sh.IrohShareHost()
    _watch, control = host.start(online_timeout=2)
    # Connect over loopback in tests: id-only tickets need network discovery,
    # so dial the host's real direct address instead.
    monkeypatch.setattr(sh, "_connect_addr", lambda _i, _b: host._endpoint.addr())
    try:
        port, joined_sid = sh.start_join_bridge(control, name="bridge-test")
        # The bridge surfaces the resume key (what HERMES_TUI_RESUME needs).
        assert joined_sid == server._sessions[sid]["session_key"]
        frames = asyncio.run(_ws_client(
            port,
            [{"jsonrpc": "2.0", "id": 10, "method": "commands.catalog", "params": {}}],
        ))
    finally:
        host.stop()

    # gateway.ready was forwarded host -> bridge -> WS client.
    assert any(
        f.get("method") == "event" and f.get("params", {}).get("type") == "gateway.ready"
        for f in frames
    ), "client never saw gateway.ready through the bridge"

    # The read-only request round-tripped the full chain and came back.
    assert any(f.get("id") == 10 for f in frames), (
        f"no commands.catalog response through the bridge; "
        f"frames={[f.get('id') or f.get('params', {}).get('type') for f in frames]}"
    )
