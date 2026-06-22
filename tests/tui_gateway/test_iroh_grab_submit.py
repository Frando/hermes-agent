"""A control joiner that grabs can drive the agent; one that has not cannot.

Exercises the wire path end to end over iroh: hello -> /grab (intercepted by the
acceptor) -> prompt.submit must be authorized (not refused), while a control
client that has not grabbed is refused. Skips without iroh.
"""

import asyncio
import json

import pytest

iroh = pytest.importorskip("iroh")

from tui_gateway import iroh_share as sh  # noqa: E402
from tui_gateway import server  # noqa: E402


async def _client(ticket, requests, *, grab=False):
    base, token = sh._split_ticket(ticket)
    ep = await iroh.Endpoint.bind(
        iroh.EndpointOptions(preset=iroh.preset_n0_disable_relay(), alpns=[sh.ALPN])
    )
    conn = await ep.connect(sh._connect_addr(iroh, base), sh.ALPN)
    bi = await conn.open_bi()
    recv, send = bi.recv(), bi.send()
    reader = sh._LineReader(recv)
    frames = []

    async def drain():
        while True:
            m = await reader.next()
            if m is None:
                break
            frames.append(m)

    task = asyncio.create_task(drain())

    async def write(obj):
        await send.write_all((json.dumps(obj) + "\n").encode("utf-8"))

    await write({"type": "hello", "token": token, "name": "ctl"})
    await asyncio.sleep(0.4)
    if grab:
        await write({"jsonrpc": "2.0", "id": 5, "method": "slash.exec", "params": {"command": "grab"}})
        await asyncio.sleep(0.4)
    for req in requests:
        await write(req)
        await asyncio.sleep(0.4)
    await asyncio.sleep(0.4)
    task.cancel()
    await ep.close()
    return frames


def _resp(frames, rid):
    return next((f for f in frames if f.get("id") == rid), None)


def test_grab_authorizes_submit_else_refused(monkeypatch):
    monkeypatch.setattr(iroh, "preset_n0", iroh.preset_n0_disable_relay)
    monkeypatch.setattr(server, "_stdio_transport", server._stdio_transport)
    server.enable_sharing_fanout()
    server.dispatch({"jsonrpc": "2.0", "id": 1, "method": "session.create", "params": {}})
    sid = server.active_shared_session_id()

    host = sh.IrohShareHost()
    _watch, control = host.share_active_session(online_timeout=2)
    # Connect over loopback in tests: id-only tickets need network discovery,
    # so dial the host's real direct address instead.
    monkeypatch.setattr(sh, "_connect_addr", lambda _i, _b: host._endpoint.addr())
    try:
        # Control client that grabs: prompt.submit must be authorized.
        grabbed = asyncio.run(_client(
            control,
            [{"jsonrpc": "2.0", "id": 10, "method": "prompt.submit",
              "params": {"session_id": sid, "text": "drive please"}}],
            grab=True,
        ))
        # Control client that has NOT grabbed: prompt.submit must be refused.
        not_grabbed = asyncio.run(_client(
            control,
            [{"jsonrpc": "2.0", "id": 20, "method": "prompt.submit",
              "params": {"session_id": sid, "text": "should be refused"}}],
            grab=False,
        ))
    finally:
        host.stop()

    grab_resp = _resp(grabbed, 5)
    assert grab_resp, f"no grab response: {grabbed}"
    grab_out = grab_resp.get("result", {}).get("output", "")
    assert "control this session" in grab_out, f"grab feedback missing: {grab_resp}"

    submit_resp = _resp(grabbed, 10)
    assert submit_resp is not None, "no response to the grabbed submit"
    assert "error" not in submit_resp or submit_resp.get("error", {}).get("code") != 4030, (
        f"grabbed control submit was refused: {submit_resp}"
    )

    refused = _resp(not_grabbed, 20)
    assert refused is not None and refused.get("error", {}).get("code") == 4030, (
        f"un-grabbed submit should be refused with 4030, got: {refused}"
    )
