# RFD: Peer-to-peer session sharing over iroh

- Status: Implemented on `new-tui` (pushed as `Frando/iroh-share`)
- Author: Frando
- Date: 2026-06-22

## Summary

`hermes share` lets a second person join a running Hermes session from another
machine and watch it live or, with the right ticket, drive the agent. The host
runs `hermes share`; the gateway binds an iroh endpoint and prints two join
tickets. A guest runs `hermes join <ticket>` and gets the real Ink TUI attached
to the host's live session over a direct peer-to-peer connection. There is no
relay server to operate and no account system: the ticket is the credential.

Sharing happens at the gateway's JSON-RPC protocol level, not by mirroring
screen output. Guests receive the same structured event stream the host's TUI
receives, so a guest renders the session with full local fidelity and, when
holding control, submits prompts and slash commands as first-class requests.

## Motivation

Pairing on an agent session has no good answer today. Screen-sharing tools send
pixels, which means the guest cannot scroll independently, cannot copy text
cleanly, and cannot type into the session without the host handing over their
whole machine. A terminal multiplexer like `tmux` shares a byte stream rather
than structure, so the guest sees the host's exact viewport and inherits its
size, and there is still no notion of who is allowed to drive.

We want something closer to a shared document: both people see the same session,
the guest's terminal is its own, and control over the agent is explicit and
exchangeable. We also want it to work across networks without asking the user to
run infrastructure, which is what iroh's direct-connection model with discovery
gives us.

An earlier branch (`frando/feat-share-sessions-iroh`) prototyped this by
mirroring rendered screen text. It proved the iroh transport but locked the
guest into the host's view and could never support independent interaction. This
design replaces that approach with protocol-level sharing while keeping its
user-facing surface: watch and control tickets, one controller at a time, and
`/grab` to take control.

## Background

The Hermes TUI is an Ink (React) front end that talks to a Python backend, the
*gateway*, over newline-delimited JSON-RPC. The TUI never runs the agent
directly: it sends requests like `prompt.submit` and `session.resume`, and it
renders events like `message.delta` and `tool.start` that the gateway emits. The
gateway already abstracts its output sink behind a `Transport` interface, and it
already supports more than one transport per session through a `FanoutTransport`.
That existing seam is what makes protocol-level sharing tractable: a guest is
just another transport attached to a session, fed by the same dispatcher.

## Design

### Topology

The host gateway runs an *acceptor*, `IrohShareHost`, on a background thread with
its own asyncio loop. The acceptor binds an iroh endpoint, accepts incoming
connections, and bridges each guest into the running gateway. Binding happens off
the main thread so relay and discovery latency never delays `gateway.ready` for
the host's own TUI.

A guest runs a *join bridge*, `start_join_bridge`, also on a background thread.
The bridge connects to the host over iroh and exposes that connection as a local
WebSocket on `127.0.0.1`. The guest then launches the ordinary Ink TUI pointed at
that WebSocket, so the TUI code needs no knowledge of iroh. The bridge pumps
JSON frames between the local TUI and the iroh stream.

Every guest request flows through the gateway's existing `server.dispatch`
unchanged. Sharing adds an admission and authorization layer in front of
`dispatch`; it does not fork the request-handling logic.

### Tickets and discovery

A ticket has the form `<endpoint-id>/<token>`. The endpoint id is the host's
iroh node identity, encoded as lowercase base32 without padding, which is 52
characters. The token is a random URL-safe string with 96 bits of entropy. The
ticket carries no network addresses: iroh's `n0` discovery resolves the endpoint
id to a reachable address at connect time, so a ticket stays valid as the host's
network changes.

The host mints two tokens at startup, one for the watch role and one for the
control role, and embeds each in its own ticket. The ALPN `hermes/tui-share/1`
identifies the protocol on the wire and versions it for future changes.

### Roles and the wire protocol

A guest opens a bidirectional stream and sends a `hello` carrying its token and a
display name. The host validates the token with `secrets.compare_digest` and
assigns the matching role, refusing the connection if neither token matches. The
host replies with a `welcome` that names the session to attach to and the current
controller, then immediately sends `gateway.ready`. After that, the connection
carries ordinary JSON-RPC: requests from the guest, responses and events from the
host.

The host sends `welcome` and `gateway.ready` before attaching the guest's
transport to the session fan-out. Attaching last guarantees the guest cannot
observe a session event ahead of its own `gateway.ready`, which the TUI requires
as its first frame.

### Control arbitration

Exactly one party drives the agent at a time. The acceptor tracks a single
`_controller`, where `None` means the host holds control and any other value
names the guest that does. A watch guest can never become the controller. A
control guest takes control with `/grab`, which transfers it away from whoever
held it, and control returns to the host when the controlling guest disconnects.

The host participates in this arbitration rather than standing outside it. The
gateway's `prompt.submit` consults the acceptor and refuses a submission from any
transport that is not the current controller. This is why `/grab` from a guest
actually stops the host from driving, and why control is a single exchangeable
token rather than a free-for-all.

### Session isolation

A guest is *pinned* to one session at join time. The acceptor records both the
ephemeral session id, which events and `prompt.submit` use, and the persistent
resume key, which `session.resume` uses. Any request that names a different
session is refused. A guest therefore cannot read or drive the host's other
sessions by guessing an id, and outbound events are filtered so a guest attached
to one session never sees activity from another.

## Security model

The ticket is the only credential, so the security of the feature rests on the
token and on what each role is allowed to do once admitted.

Token validation is constant-time and fails closed. There is no path to
`server.dispatch` without a valid token, the assigned role is bound to the token
that produced it, and `/grab` re-checks that the requester holds a control role
before transferring control.

Authorization is allow-list based and deny-by-default. A watch guest may call
only a small, audited set of read-only methods (`_READ_ONLY_METHODS`): the slash
command catalog, slash completion, and read-only views of the pinned session. Any
method not on that list is treated as mutating, including any method added in the
future, so a forgotten entry fails safe rather than leaking capability. A control
guest may call mutating methods only while it holds control. The allow-list
deliberately excludes methods that run an agent, return secrets such as API keys,
or read across sessions.

The control ticket is the most sensitive value the host holds, because it grants
the ability to drive the agent. We route the startup `share.info` message, which
prints the tickets, to the host's own terminal only and never through the session
fan-out, so the control ticket cannot reach an attached guest. The `share.tickets`
method, which backs the `/share` command, returns the control ticket only to the
host transport; a guest that reaches it receives the watch ticket alone.

What the model does not provide: there is no rate limiting on connection
attempts, and there is no revocation of a leaked ticket short of restarting the
share. The 96-bit token makes brute force infeasible, and the endpoint id needed
to connect is itself only present in a ticket, so these are acceptable for the
pairing use case but worth revisiting if the feature's threat model widens.

## Reliability

The acceptor and the join bridge each own an asyncio loop on a daemon thread, and
cross-thread writes from gateway worker threads are marshaled onto the loop with
`call_soon_threadsafe`. Per-guest output goes through a bounded queue that drops
the oldest frame when a slow guest falls behind, so one stalled guest cannot grow
memory without limit or block the host.

Resources are bounded and reclaimed. The acceptor caps concurrent guests, tears
down a guest's transport and connection when its read loop ends, and returns
control to the host if the controller leaves. On process exit an `atexit` hook
closes the iroh endpoint so the relay drops the host's mapping rather than
leaving it to time out. The join bridge tears itself down when the local TUI
disconnects and gives up if the TUI never connects.

Failures that matter are logged. A failed fan-out setup, an unexpected write
error to a guest, a session database row that cannot be ensured (which would
otherwise break a guest's resume), and acceptor handshake errors all reach the
log rather than being swallowed.

## CLI and user experience

The host runs `hermes share`, which sets `HERMES_TUI_SHARE` and launches the TUI;
the gateway installs the acceptor when that variable is set. The tickets print in
the transcript on startup, and `/share` reprints them on demand. A guest runs
`hermes join <ticket>`, which starts the bridge and launches the TUI against it.
A control guest types `/grab` to take control. The Ink TUI gained a persisted
share banner so the tickets survive the startup intro render, plus handling for
the `share.info` and `message.user` events that sharing introduces.

## Tradeoffs and alternatives

Sharing at the protocol level rather than the screen level is the central
decision. It costs more surface area, since every method a guest can call is part
of the security boundary, and it ties the wire format to the gateway's JSON-RPC
contract. In return the guest gets a real, independent TUI rather than a mirror,
control becomes a first-class concept, and the feature extends naturally as the
protocol grows. We judged that worth the larger boundary, and we contained the
boundary with the deny-by-default allow-list.

Running the guest's TUI against a local WebSocket, rather than teaching the TUI to
speak iroh directly, keeps all iroh code in the Python backend and leaves the Ink
front end unaware of the transport. The cost is an extra hop and a loopback socket
per guest, which is negligible next to the network path to the host.

We use iroh's `n0` discovery so a ticket needs no embedded addresses and survives
the host changing networks. The tradeoff is a dependency on that discovery service
for the initial rendezvous, after which the connection is direct.

## Open questions and future work

- Sharing is process-wide rather than per-session: a gateway in share mode mints
  one watch and one control token for the whole process, and a joiner is pinned
  to whichever session is most recently active. A gateway running several
  sessions in parallel cannot share one without exposing the others, and two
  joiners connecting at different times can land on different sessions. Making
  sharing a per-session operation, driven by `/share` and `/unshare`, resolves
  this and is the subject of a follow-up design.
- Control arbitration is currently enforced only on `prompt.submit`. A host can
  still drive the agent through other methods such as `slash.exec` while a guest
  holds control. The gate should cover every agent-driving method.
- The shared session is selected as the host's most recently active session and
  recomputed per connection, which can pin two guests to different sessions or
  share a session the host did not intend. The session should be pinned once.
- The share banner is not preserved by the TUI's transcript cap, so in very long
  sessions it can be pruned and re-added at the bottom.
</content>
