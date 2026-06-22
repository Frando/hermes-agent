# RFD: Per-session peer-to-peer session sharing over iroh

- Status: Implemented on `Frando/iroh-share-per-session`
- Author: Frando
- Date: 2026-06-22

## Summary

`hermes share` lets a second person join a running Hermes session from another
machine and watch it live or, with the right ticket, drive the agent. Sharing is
a per-session operation. A session starts unshared; the host shares it with
`/share`, which prints two join tickets, and stops with `/unshare`. A guest runs
`hermes join <ticket>` and gets the real Ink TUI attached to the host's live
session over a direct peer-to-peer connection. There is no relay server to
operate and no account system: the ticket is the credential.

One iroh endpoint is shared across the whole process, but the tokens are minted
per session and held in a process-wide map from token to session. A joiner
presents a token, the host looks it up to find the session and role, and pins
the joiner to that session. Control over the agent is negotiated per session, so
a host running several sessions can share one without exposing the others, and
each shared session has its own controller.

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

Sharing must also be coupled to the session, not to the process. A Hermes
gateway can run several sessions in parallel. If sharing is process-wide, the
host cannot share one session while keeping the others private, and a joiner has
no well-defined session to attach to: an earlier design pinned joiners to the
most recently active session, which drifts as the host switches focus and can
land two joiners on two different sessions. Treating `/share` as an operation on
the current session removes the ambiguity and gives the host per-session control
over what is exposed.

## Background

The Hermes TUI is an Ink (React) front end that talks to a Python backend, the
*gateway*, over newline-delimited JSON-RPC. The TUI never runs the agent
directly: it sends requests like `prompt.submit` and `session.resume`, and it
renders events like `message.delta` and `tool.start` that the gateway emits. The
gateway already abstracts its output sink behind a `Transport` interface, and it
already supports more than one transport per session through a `FanoutTransport`.
That existing seam is what makes protocol-level sharing tractable: a guest is
just another transport attached to a session, fed by the same dispatcher.

The fan-out is process-wide, and every session's events flow through it. What
keeps a guest from seeing another session's events is a per-frame filter on each
guest's transport: a session event reaches a guest only when its `session_id`
matches the session the guest is pinned to. This filter is the unit of session
isolation, and the per-session design builds directly on it.

## Design

### Sharing model

A session starts unshared. The endpoint is not even bound until the first share.
Three operations drive the lifecycle, all scoped to the current session:

- `/share` mints a watch token and a control token for the session, binds the
  shared iroh endpoint if it is not already up, and registers both tokens in a
  process-wide map from token to `(session, role)`. It returns and prints the two
  join tickets.
- `/unshare` removes the session's tokens from the map and disconnects any
  joiners attached to it.
- `/grab` takes control of the session the caller is attached to.

The host holds a single `IrohShareHost` for the process. It owns the endpoint and
a registry of `_SharedSession` records keyed by the session's persistent key,
each carrying that session's tokens and its current controller. The token map is
the inverse index the acceptor consults on every connection.

### Tickets and discovery

A ticket has the form `<endpoint-id>/<token>`. The endpoint id is the host's iroh
node identity, encoded as lowercase base32 without padding, which is 52
characters. The token is a random URL-safe string with 96 bits of entropy. The
ticket carries no network addresses: iroh's `n0` discovery resolves the endpoint
id to a reachable address at connect time, so a ticket stays valid as the host's
network changes. Because the endpoint is shared, every ticket for every shared
session carries the same endpoint id and differs only in the token.

### Roles and the wire protocol

A guest opens a bidirectional stream and sends a `hello` carrying its token and a
display name. The host looks the token up in the map. A match yields the session
and the role; no match means the token is wrong or the session is no longer
shared, and the connection is refused. The host replies with a `welcome` naming
the session to attach to and its current controller, then immediately sends
`gateway.ready`. After that the connection carries ordinary JSON-RPC: requests
from the guest, responses and events from the host. The ALPN
`hermes/tui-share/1` identifies and versions the protocol on the wire.

The host sends `welcome` and `gateway.ready` before attaching the guest's
transport to the fan-out. Attaching last guarantees the guest cannot observe a
session event ahead of its own `gateway.ready`, which the TUI requires as its
first frame.

### Control arbitration

Exactly one party drives each shared session at a time. Each `_SharedSession`
tracks its own controller, where `None` means the host holds control and a guest
value means that guest does. A watch guest can never become the controller. A
control guest takes control with `/grab`, which transfers it away from whoever
held it for that session, and control returns to the host when the controlling
guest disconnects. Because the controller is per session, grabbing control of one
session has no effect on another.

The host participates in this arbitration rather than standing outside it. The
gateway's `prompt.submit` consults the host with the submitted session's key and
refuses a submission from any transport that is not that session's controller.
When the session is not shared, the check is a no-op, so the solo path is
unaffected.

### Session isolation

A guest is pinned to the session its token maps to, recorded at join time as both
the ephemeral session id used by events and `prompt.submit` and the persistent
resume key used by `session.resume`. Any request naming a different session is
refused, so a guest cannot read or drive the host's other sessions by guessing an
id. Outbound session events are filtered by the per-frame filter, so a guest
attached to one shared session never sees another session's activity even though
all events pass through the one fan-out.

## Security model

The ticket is the only credential, so the security of the feature rests on the
token and on what each role is allowed to do once admitted.

Token resolution is constant-time and fails closed. There is no path to
`server.dispatch` without a token that maps to a shared session, the role is
bound to the token, and `/grab` re-checks that the requester holds a control role
before transferring control. Unsharing a session removes its tokens, so an old
ticket stops working the moment the host runs `/unshare`.

Authorization is allow-list based and deny-by-default. A watch guest may call
only a small, audited set of read-only methods: the slash command catalog, slash
completion, and read-only views of the pinned session. Any method not on that
list is treated as mutating, including any method added in the future, so a
forgotten entry fails safe rather than leaking capability. A control guest may
call mutating methods only while it holds control of its session. The allow-list
deliberately excludes methods that run an agent, return secrets such as API keys,
or read across sessions.

Two facts about frame routing make the per-session, multi-session case safe.
First, the fan-out forwards only events to guests, never responses to the host's
own requests. A response carries an `id` and no `method`, and may hold host-only
data such as the API keys returned by `config.get` or the control ticket returned
by `share.start`; fanning it out would leak it to every attached guest, and with
several sessions sharing one fan-out that is a real exposure. A guest still
receives responses to its own requests, because those are written to its
transport directly rather than through the fan-out. Second, the join tickets are
delivered to the host through the fan-out's primary, which is the host's own
terminal, and rendered from a `share.info` event. They never travel in an RPC
response and never reach the fan-out's members.

What the model does not provide: there is no rate limiting on connection
attempts, and there is no revocation of an individual leaked ticket short of
unsharing the session. The 96-bit token makes brute force infeasible, and the
endpoint id needed to connect is itself only present in a ticket, so these are
acceptable for the pairing use case but worth revisiting if the threat model
widens.

## Reliability

The acceptor owns an asyncio loop on a daemon thread, and cross-thread writes
from gateway worker threads are marshaled onto the loop with
`call_soon_threadsafe`. Per-guest output goes through a bounded queue that drops
the oldest frame when a slow guest falls behind, so one stalled guest cannot grow
memory without limit or block the host.

The endpoint binds lazily on the first `/share`, so a user who never shares pays
no iroh cost. Because `/share` binds the endpoint, it runs on the gateway's
worker pool rather than inline, so bind and relay latency never blocks the host's
input loop. Resources are bounded and reclaimed: the acceptor caps concurrent
guests, `/unshare` disconnects a session's guests, a guest's transport and
connection are torn down when its read loop ends, and control returns to the host
if a controller leaves. On process exit an `atexit` hook closes the iroh endpoint
so the relay drops the host's mapping rather than leaving it to time out.

Failures that matter are logged: a failed fan-out setup, an unexpected write
error to a guest, a session database row that cannot be ensured (which would
otherwise break a guest's resume), and acceptor handshake errors.

## CLI and user experience

The host shares the current session with `/share` and stops with `/unshare`. A
control guest types `/grab` to take control. `hermes share` remains a
convenience: it launches the TUI with `HERMES_TUI_SHARE` set, and the TUI issues
`share.start` for the initial session once it is ready, so the common case of
"launch already sharing" still works without a manual `/share`. The tickets
render as a persisted banner that survives the startup intro; `/unshare` clears
it. A guest runs `hermes join <ticket>`.

## Tradeoffs and alternatives

Sharing at the protocol level rather than the screen level is the central
decision. It costs more surface area, since every method a guest can call is part
of the security boundary, and it ties the wire format to the gateway's JSON-RPC
contract. In return the guest gets a real, independent TUI rather than a mirror,
control becomes a first-class concept, and the feature extends naturally as the
protocol grows. We judged that worth the larger boundary, and we contained the
boundary with the deny-by-default allow-list.

Sharing one endpoint across all sessions, rather than one endpoint per shared
session, keeps a single relay registration and discovery identity for the host
and makes a ticket's endpoint id the same everywhere. The cost is that all shared
sessions' events pass through one fan-out and are separated by the per-frame
filter rather than by separate transports. The filter already existed and is
cheap, so this is the simpler and lighter choice.

Running the guest's TUI against a local WebSocket, rather than teaching the TUI to
speak iroh directly, keeps all iroh code in the Python backend and leaves the Ink
front end unaware of the transport. The cost is an extra hop and a loopback
socket per guest, which is negligible next to the network path to the host.

## Open questions and future work

- Control arbitration is enforced on `prompt.submit` but not yet on the other
  agent-driving methods such as `slash.exec` and `session.steer`. Guests are
  still safe, because those methods are denied to a non-controller by the
  allow-list, but a host can drive through them while a guest holds control. The
  gate should cover every agent-driving method.
- The TUI holds a single `shareInfo` banner, so a host sharing several sessions
  sees only the most recently shared session's tickets in the banner. The tickets
  for any shared session remain available by running `/share` in that session.
- The fan-out now withholds responses from guests. This is the right default for
  a watch and control feature, but it is a behavioral change to the transport
  worth confirming against any future code that expects a guest to observe a
  response to one of the host's requests.
