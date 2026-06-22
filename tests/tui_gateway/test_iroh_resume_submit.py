"""Reproduce the joiner flow: resume the shared session, grab, then submit.

The real TUI resumes the session named in the welcome and then submits using the
session id from the RESUME response. This checks that id is the same live
session the gateway can drive, i.e. prompt.submit is not "session not found".
"""

import asyncio
import json

import pytest

iroh = pytest.importorskip("iroh")

from tui_gateway import iroh_share as sh  # noqa: E402
from tui_gateway import server  # noqa: E402


async def _run(control):
    base, token = sh._split_ticket(control)
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

    async def write(o):
        await send.write_all((json.dumps(o) + "\n").encode("utf-8"))

    await write({"type": "hello", "token": token, "name": "joiner"})
    await asyncio.sleep(0.4)
    welcome = next((f for f in frames if f.get("type") == "welcome"), None)
    wsid = welcome["session_id"]

    # Resume like the real TUI (HERMES_TUI_RESUME = welcome session id).
    await write({"jsonrpc": "2.0", "id": 8, "method": "session.resume",
                 "params": {"session_id": wsid, "cols": 80}})
    await asyncio.sleep(0.8)
    resume = next((f for f in frames if f.get("id") == 8), None)
    rsid = (resume or {}).get("result", {}).get("session_id")

    # Grab, then submit using the resume response's session id.
    await write({"jsonrpc": "2.0", "id": 5, "method": "slash.exec", "params": {"command": "grab"}})
    await asyncio.sleep(0.4)
    await write({"jsonrpc": "2.0", "id": 10, "method": "prompt.submit",
                 "params": {"session_id": rsid, "text": "drive"}})
    await asyncio.sleep(0.6)
    submit = next((f for f in frames if f.get("id") == 10), None)
    task.cancel()
    await ep.close()
    return wsid, rsid, submit


def test_resume_then_submit_finds_session(monkeypatch):
    monkeypatch.setattr(iroh, "preset_n0", iroh.preset_n0_disable_relay)
    monkeypatch.setattr(server, "_stdio_transport", server._stdio_transport)
    server.enable_sharing_fanout()
    server.dispatch({"jsonrpc": "2.0", "id": 1, "method": "session.create", "params": {}})
    created = server.active_shared_session_id()

    key = server._sessions[created]["session_key"]

    host = sh.IrohShareHost()
    _watch, control = host.start(online_timeout=2)
    # Connect over loopback in tests: id-only tickets need network discovery,
    # so dial the host's real direct address instead.
    monkeypatch.setattr(sh, "_connect_addr", lambda _i, _b: host._endpoint.addr())
    try:
        wsid, rsid, submit = asyncio.run(_run(control))
    finally:
        host.stop()

    print("created=", created, "key=", key, "welcome=", wsid, "resume=", rsid, "submit=", submit)
    # The welcome carries the persistent resume key (the DB id), not the
    # ephemeral id, so the joiner's session.resume reattaches to the live session.
    assert wsid == key, f"welcome should carry the resume key {key}, got {wsid}"
    # Resume reattaches to the live session and returns its ephemeral id to drive.
    assert rsid == created, f"resume should return the ephemeral id {created}, got {rsid}"
    # Submitting with that ephemeral id finds the session (no 'session not found').
    assert submit is not None, "no prompt.submit response"
    err = submit.get("error")
    assert not (err and err.get("code") == 4001), f"prompt.submit said session not found: {submit}"
