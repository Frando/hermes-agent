---
sidebar_position: 8
title: "Session sharing"
description: "Share a live TUI session over iroh so others can watch or take control"
---

# Session sharing over iroh

A running TUI session can be shared so other devices and people watch it live
and, with the right ticket, take control and drive the agent. Sharing works at
the gateway protocol level: a joiner runs the real Hermes TUI attached to the
host's gateway over an iroh connection, so it renders the same structured
transcript, streaming, and tool activity the host sees rather than a copy of the
host's screen. The connection is peer to peer and end to end encrypted, and it
works across networks without opening ports.

Sharing is built in; there is nothing to install. The iroh endpoint binds the
first time you share a session, so a session you never share costs nothing.

## Sharing a session

In any `hermes --tui` session, type:

```
/share
```

This prints two tickets:

```
Sharing this session over iroh.
  watch:   hermes join zcpyh3fv...atzak7q/bDUU8AgB_585fUrx
  control: hermes join zcpyh3fv...atzak7q/5L6GrXAwlH3YRXcB
```

A ticket is `<endpoint-id>/<token>`: the host's iroh endpoint id and a role
token. Both tickets reach the same session and differ only in the role they
grant. Hand out the watch ticket to people who should only follow along, and the
control ticket to people you trust to drive the agent. Run `/share` again at any
time to reprint the tickets, and `/unshare` to stop sharing and disconnect the
joiners.

Sharing is per session. Each session you share gets its own pair of tickets off
the one shared endpoint, so sharing one session never exposes another.

## Joining

On another machine, run the command from the ticket:

```
hermes join --name frando zcpyh3fv...atzak7q/5L6GrXAwlH3YRXcB
```

This opens the full Hermes TUI attached to the host's session. A watch ticket
renders the session as it happens. A control ticket can additionally take
control with `/grab` and then type to the agent. A joiner cannot reshare or
unshare the session it joined; only the host controls sharing.

## Control

The host always drives its own session. A control joiner takes control with
`/grab`; while it holds control it can submit prompts, run slash commands, and
respond to approvals. Another control joiner can `/grab` it back, and the host
can reclaim control with `/grab` too. Watch tickets can never control. Everyone
is notified when control moves.

## How it works

The TUI already speaks newline-delimited JSON-RPC to its Python gateway, and the
gateway can drive that protocol over more than one transport (stdio for the
local TUI, WebSocket for the dashboard). Sharing adds an iroh transport: the
host's gateway accepts joiner connections and bridges them into the running
session through the same dispatcher, and the session's event stream fans out to
every attached client. `hermes join` runs a small local bridge that connects to
the host over iroh and exposes it as a local WebSocket the TUI attaches to, so
the joiner is the real TUI with no special client.

## Trust model

The tickets are bearer capabilities. Anyone with the control ticket holds the
same authority as someone at your keyboard: they can run slash commands, send
messages to the agent, and trigger any tool the agent can use, including the
shell and file access. Anyone with either ticket can read everything the shared
session prints, though not your other sessions. iroh authenticates and encrypts
the transport, but it does not know who is on the other end. Share tickets only
with people you mean to, and `/unshare` when you are done.
