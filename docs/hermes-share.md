# RFD: hermes-share

- Status: implemented
- Authors: Frando
- Protocol: `hermes-share/0`

## Summary

`hermes-share` lets people join a running Hermes agent session from
another machine over a direct peer-to-peer connection, watch it live, and with
the right ticket drive the agent. The host is the existing Hermes gateway: any
`hermes --tui` session can be shared. A joiner needs no Python and no Hermes
install. Two clients exist on top of one small Rust library: a terminal join
that reuses the real Ink TUI, and a Dioxus app that runs on web, desktop,
Android, and iOS.

Sharing is peer-to-peer over iroh. There is no relay to operate and no account
system: a ticket is the only credential. The host binds one iroh endpoint and
mints a watch token and a control token per shared session. A joiner presents a
token, the host grants the matching role, and the joiner is pinned to that one
session. Watchers may call a small, audited set of read-only methods. A
controller may drive the agent while it holds control, which is negotiated so
that exactly one party drives at a time.

Sharing happens at the gateway's JSON-RPC protocol level, not by mirroring
screen output. A joiner receives the same structured event stream the host's
TUI receives, so it renders the session with full local fidelity and, when it
holds control, submits prompts and slash commands as first-class requests.

## Motivation

Pairing on an agent session has no good answer today. Screen-sharing tools send
pixels: the guest cannot scroll independently, cannot copy text cleanly, and
cannot type into the session without the host handing over the whole machine. A
terminal multiplexer like `tmux` shares a byte stream rather than structure, so
the guest inherits the host's exact viewport and size, and there is still no
notion of who is allowed to drive.

We want something closer to a shared document. Everyone sees the same session,
each renders it in its own client at its own size, and control over the agent is
explicit and exchangeable: many participants may watch at once, and exactly one
drives at a time. It should work across networks without asking anyone to run
infrastructure, which is what iroh's direct-connection model with discovery
provides. And a joiner should not be tied to one device or runtime: the same
session should be reachable from a terminal or a phone.

The host already abstracts its output behind a `Transport` interface and already
supports more than one transport per session through a `FanoutTransport`. A
joiner is just another transport attached to a session, fed by the same
dispatcher. That existing seam is what makes protocol-level sharing tractable and
keeps the host-side change small.

## Background: the gateway

The Hermes TUI is an Ink front end that talks to a Python backend, the
*gateway*, over newline-delimited JSON-RPC. The TUI never runs the agent
directly. It sends requests such as `prompt.submit` and `session.resume`, and it
renders events such as `message.delta` and `tool.start` that the gateway emits.

A *session* has two identities, and the distinction is load-bearing for the
protocol below. The *ephemeral session id* is the in-memory key in the gateway's
`_sessions` dict, an eight-character hex string regenerated each time the session
is bound to a live agent. The *resume key* is the persistent database id, stable
across reconnects. Events carry the ephemeral id in `params.session_id`;
`prompt.submit` is keyed by the ephemeral id; `session.resume` takes the resume
key and returns the ephemeral id. Confusing the two is the most common way to
break a joiner, so the protocol is precise about which id goes where.

The gateway is multi-session-capable: `_sessions` can hold many live sessions at
once, each with its own agent, model, workspace, and slash worker. The TUI shows
one at a time. A separate resource bound limits how many sessions may be *active*
at once; this is discussed under Limitations.

## The `hermes-share/0` protocol

### Transport and framing

The host binds one iroh endpoint per gateway with the ALPN `hermes-share/0`. A
joiner binds its own endpoint and dials the host's endpoint id. Discovery
resolves the id to a reachable address at connect time, so no network addresses
are carried anywhere.

The host accepts exactly one bidirectional stream per connection. A client that
wants two sessions to the same host therefore opens two connections; pooling by
host would leave the second stream unanswered. The Rust `Client` follows this
rule: every `join` opens a fresh connection.

Every frame is one line of UTF-8 JSON terminated by `\n`. The whole stream is
JSON-RPC 2.0, the handshake included. A frame with a `method` of `"event"` is a
gateway event (a JSON-RPC notification); a frame with an `id` and a `result` or
`error` is a response to a request. The host caps a single line at one megabyte
and silently drops a frame that does not parse, so a malformed line never wedges
the stream.

### Tickets

A ticket names the host endpoint and carries the role token:

```
hermes:<base32-endpoint-id>/<token>
```

The endpoint id is the host's 32-byte iroh identity, encoded as lowercase base32
without padding, which is 52 characters. The token is a URL-safe random string
with 96 bits of entropy. The two are joined by the last `/`, and neither part
contains a `/`, so splitting on the final separator is unambiguous. The
`hermes:` scheme makes a ticket a clickable link that the apps register; parsing
also accepts `hermes://`, and a bare `<endpoint-id>/<token>` with no scheme.

Every ticket for every session on one host shares the same endpoint id and
differs only in the token, because the endpoint is process-wide. The web app
carries the ticket in a URL fragment (`.../#<endpoint-id>/<token>`), which never
reaches a server and survives static hosting, so a share link is a natural place
for it.

### The handshake

The joiner opens the stream and sends a `hello` request. Its id is the constant
`0`, which the host echoes so the joiner can recognise the welcome among any
other first frame:

```json
{"jsonrpc":"2.0","id":0,"method":"hello","params":{"token":"<token>","name":"alice"}}
```

The host resolves the token in constant time against every registered token. A
match yields the shared session and the role; no match means the ticket is wrong
or the host has stopped sharing that session, and the connection is refused with
error `4030`. On success the host replies with the welcome as the `hello`
response:

```json
{"jsonrpc":"2.0","id":0,"result":{"role":"control","session_id":"<resume-key>","controller":"host"}}
```

The `role` is `"control"` or `"watch"`. The `session_id` in the welcome is the
*resume key*, not the ephemeral id: it is what the joiner passes to
`session.resume` to reattach to this exact live session. The `controller` names
who currently drives, where `"host"` means the host's local pane holds control.

The host sends the welcome and then a `gateway.ready` event before attaching the
joiner to the session fan-out. Attaching last guarantees the joiner cannot
observe a session event ahead of its own `gateway.ready`, which clients rely on
as the first frame. After this the connection is ordinary JSON-RPC.

### The ephemeral-versus-persistent id rule

The welcome gives the joiner the resume key. The joiner's first real request is
`session.resume` keyed by that resume key:

```json
{"jsonrpc":"2.0","id":1,"method":"session.resume","params":{"session_id":"<resume-key>"}}
```

The resume response, and every subsequent session event, carries the *ephemeral*
id in `result.session_id` or `params.session_id`. This is the id the joiner must
use for `prompt.submit` and `grab`. Submitting a prompt keyed by the resume key
fails: the gateway looks prompts up by the ephemeral id. The host pins a joiner
to both forms, so a request naming either the ephemeral id or the resume key is
accepted and anything else is refused, but a joiner that wants its prompt to run
must learn and use the ephemeral id. Clients capture it from the first frame that
carries one and use it thereafter.

### Roles

A watch ticket may call only an audited, read-only allow-list: the static slash
command catalog, command name resolution, slash completion, and read-only views
of the pinned session (`session.resume`, `session.history`, `session.status`,
`session.usage`). Every other method, including any method added later, is
treated as mutating. A watcher is refused it, and a controller may call it only
while it holds control. Failing closed is the point: a forgotten allow-list entry
denies capability rather than leaking it. The list deliberately excludes anything
that runs an agent, returns secrets such as API keys, or reads across sessions.

### Events

Events are JSON-RPC notifications with `method` `"event"` and a `params` object
of `{type, session_id, payload}`. A client models the types it renders and folds
anything else to an ignored "other" case, so an unmodelled event never breaks it.
The types that matter:

- `gateway.ready`: the first frame after the welcome.
- `message.user`: a prompt entered by another participant. The gateway echoes a
  prompt only to the *other* participants, so a client renders its own prompt
  optimistically.
- `message.start`, `message.delta`, `message.complete`: a streaming assistant
  turn. A run of deltas between a start and a complete builds the answer; a
  complete may carry the final text, which overrides the accumulated deltas.
- `reasoning.delta` (also `thinking.delta`): streaming reasoning, rendered dim
  before the answer.
- `tool.start`, `tool.complete`: tool activity, named by `payload.name`.
- `status.update`: a short agent status line such as "compacting".
- `notification.show`: the gateway's toast channel, carrying join and leave
  notices and control changes. Share notices are prefixed `[share] `.
- `control.update`: the structured control holder, `{"controller": "<name>"}`,
  where `"host"` means the host drives. This is the precise counterpart to the
  toast, so a client tracks the holder without parsing text.
- `error`: an error to surface in the transcript.

A representative streaming turn:

```json
{"jsonrpc":"2.0","method":"event","params":{"type":"message.start","session_id":"a1b2c3d4","payload":{}}}
{"jsonrpc":"2.0","method":"event","params":{"type":"message.delta","session_id":"a1b2c3d4","payload":{"delta":"Hel"}}}
{"jsonrpc":"2.0","method":"event","params":{"type":"message.delta","session_id":"a1b2c3d4","payload":{"delta":"lo"}}}
{"jsonrpc":"2.0","method":"event","params":{"type":"message.complete","session_id":"a1b2c3d4","payload":{"text":"Hello"}}}
```

### Requests

A controller drives the agent with a handful of requests, all keyed by the
ephemeral session id:

```json
{"jsonrpc":"2.0","id":7,"method":"prompt.submit","params":{"session_id":"a1b2c3d4","text":"refactor this"}}
```

`command.dispatch` with name `grab` takes control (see below). `image.attach_bytes`
uploads an image by its base64 bytes; the host queues it and includes it in the
next prompt on its own, so no reference has to be threaded into the message text.
`file.attach` uploads a non-image file by a `data:` URL and returns an `@file:`
reference in `result.ref_text` for the client to inject into the prompt:

```json
{"jsonrpc":"2.0","id":1000001,"method":"file.attach","params":{"session_id":"a1b2c3d4","name":"notes.txt","data_url":"data:text/plain;base64,..."}}
```

### Control negotiation

Exactly one party drives a shared session at a time. The host holds control
initially. A controller takes it with `grab`, sent as `command.dispatch` with
`{"name":"grab"}`. The host intercepts a joiner's grab in the acceptor by method
and name before it reaches the dispatcher, transfers control to that joiner, and
replies in the exec-dispatch shape so clients render the outcome as a visible
line:

```json
{"jsonrpc":"2.0","id":2,"result":{"type":"exec","output":"You now control this session. Type to drive the agent."}}
```

A watch ticket cannot take control; its grab is refused. When the controlling
joiner disconnects, control returns to the host. Every control change is
announced two ways to the session's participants: a `notification.show` toast for
humans and a structured `control.update` carrying the new holder. The host
participates in the same arbitration: the gateway's `prompt.submit` consults the
share host with the session's key and refuses a submission from any transport
that is not the current controller, the host's own stdio transport included, so
two panes cannot drive at once. When the session is not shared, this check is a
no-op and the solo path is unaffected.

## Host implementation

The host lives in `tui_gateway/iroh_share.py` and hooks into
`tui_gateway/server.py`. One `IrohShareHost` per gateway owns the iroh endpoint,
bound lazily on the first share so a user who never shares pays no iroh cost. The
acceptor runs an asyncio loop on a daemon thread; writes from gateway worker
threads are marshaled onto that loop with `call_soon_threadsafe`.

A `_SharedSession` record holds one session's tokens, tickets, and current
controller. The registry maps the persistent key to its `_SharedSession`
(`_shared`) and each token to its `(session, role)` (`_by_token`), guarded by a
lock because the gateway thread mutates it on share and unshare while the
acceptor reads it on hello and grab. `share.start` mints a watch and a control
token for a session and registers them; it is idempotent and refreshes the
ephemeral id if the live session was rebound. `share.stop` removes the tokens and
disconnects that session's joiners.

The fan-out is how more than one client observes one session. `enable_sharing_fanout`
upgrades the process stdio transport to a `FanoutTransport` whose primary is the
host's own frontend; each joiner is attached as an extra member via
`attach_shared_transport`. The gateway's `write_json` routes a session event to
the transport stored on that session, so it reaches the host's frontend and every
attached joiner. Two routing facts keep the multi-session case safe. The fan-out
forwards only events to extras, never responses to the host's own requests, which
can hold host-only data such as API keys or the control ticket. And each joiner's
transport applies a per-frame filter: a session event reaches a joiner only when
its `session_id` matches the session the joiner attached to, so a joiner watching
one shared session never sees the host's other sessions even though all events
pass through the one fan-out.

A joiner is pinned to its session. The acceptor's read loop rejects any request
naming a different session with error `4031`, addressable only by the pinned
session's ephemeral id or resume key. Every non-allow-listed request is
authorized against the controller before it reaches `server.dispatch`, which runs
on the gateway's worker pool so a slow handler does not stall the acceptor loop.

`share.start`, `share.stop`, and `share.status` are host-only. They are not in
the read-only allow-list, so a joiner cannot reach them, and they additionally
reject any transport that is not the process stdio transport. A joiner, even one
holding control, therefore cannot share or unshare the host's session out from
under everyone, and `share.status` returns the control ticket only to the host.

## The Rust client library

`hermes-share` is a small, runtime-agnostic library with four pieces.

`Client` binds one iroh endpoint and is cheap to clone. `Client::join` parses a
ticket, opens a fresh connection, runs the handshake, and returns three things: a
`Reader`, a `Writer`, and an `Info` carrying the granted role, the resume key,
and the current controller. Reader and writer are separate owned halves so a
caller reads frames and sends requests concurrently without a borrow conflict.
Each half holds a clone of the connection and the endpoint, so the connection
survives the `Client` being dropped. No executor is spawned; the caller owns the
loop.

The `protocol` module is the wire contract. It defines the `Event` enum, request
builders (`hello`, `session_resume`, `prompt_submit`, `grab`, `image_attach_bytes`,
`file_attach`), and tolerant parsers. `event_from_frame` folds an unmodelled type
to `Event::Other`. `session_id_from_frame` extracts the ephemeral id from an
event's `params.session_id` or a response's `result.session_id`, which is how a
client learns the id it needs for prompts. `history_from_response` reads a resume
or history response into `(role, text)` pairs, returning an empty list on an
unrecognised shape rather than erroring. The module never panics on a malformed
frame.

`Chat` is a pure fold of events into a renderable transcript with no I/O, so the
render logic is unit-tested against recorded event sequences. It holds the message
list, the role, the controller, the resume key, the ephemeral id, the connection
status, and any staged attachments. `apply` folds one `Event` into the
transcript: it seals a reasoning block before the answer, accumulates deltas into
a streaming message, and lets a `message.complete` override the accumulated text.
`take_attachments` drains staged file references for the next prompt; images carry
no reference because the host queues them itself.

`ticket` parses `hermes:<base32-id>/<token>` (and the bare and `hermes://` forms)
into an `EndpointId` and a token, the inverse of the host's base32 encoding.

## The Dioxus app

`hermes-share-app` is a Dioxus app that builds from one component tree as a web
app (wasm), a native desktop app, and Android and iOS apps; iroh runs in all of
them. It renders a list of joined chats and one chat at a time. Joining asks for
a ticket, a nick, and an optional label, or auto-joins a ticket passed on the
command line or in the web URL fragment.

One shared `Client` serves every chat. Each chat runs a connection task that owns
its `Reader` and `Writer` and a UI-side task that folds `Update`s into the chat's
`Chat`, decoupled by channels so the connection side stays `Send` and never
touches a Dioxus signal. The connection task issues `session.resume` on connect,
learns the ephemeral id from the first frame that carries one, and forwards
modelled events, history, attachment results, and grab outcomes to the UI. The
composer shows who holds control, offers a grab button when the joiner has a
control ticket but not control, and surfaces an error rather than silently
dropping a prompt sent without control. File and image upload route by MIME and
extension: images go through `image.attach_bytes`, other files through
`file.attach`, each shown as a chip until the next prompt consumes it. A watch
ticket shows a read-only note instead of the composer. On narrow screens the
session list becomes a full-screen overlay.

## Limitations and future work

### One active session per host

The gateway holds many live sessions in `_sessions`, but only one is *active* per
Hermes instance at a time. `try_acquire_active_session` bounds the active slot and
returns error `4090` when the limit is hit. The host reacts to the user's
navigation: `/new` runs `session.create`, which creates a new session and makes it
active; `/session` and the session picker run `session.activate`, which switches
which session the frontend is focused on without closing the previous one, and
`session.active_list` enumerates the live sessions to pick from.

`share.start` shares a specific session (the active one by default), and a joiner
is pinned to that one session for the life of its connection. A single shared
connection therefore follows one session. Switching the host's active session
does not move a pinned joiner: it keeps watching or driving the session it joined,
which is the intended isolation, but it also means a joiner cannot follow the host
across a `/new` or a session switch on the same connection. Following a different
session means joining that session's ticket on a new connection.

### Remote session management (the manage-sessions plan)

A joiner today must be handed a ticket for a session that already exists. The
manage-sessions plan extends `hermes-share/0` so a remote client can create and
pick sessions on a host over the same protocol, with a small host-side diff,
because the gateway is already multi-session-capable.

The plan adds a `manage` role and one host-wide manage token, distinct from the
per-session watch and control tokens. A manage connection is not pinned to any
session and is not attached to a session fan-out; its welcome returns a null
session id. It may call a small control-plane method set: `manage.new_session`,
which runs `session.create` and `share_session` host-side and returns the new
session's control and watch tickets, and optionally `manage.list_sessions` to
resume an existing one. The client then opens an ordinary join connection to the
returned control ticket and drives the session exactly as today, so no new event
or drive path is needed on the manage connection. A headless launcher boots the
gateway without a TUI, mints the manage token, prints the manage ticket, and
idles, turning a host into "remote me" that a client can manage from afar.

The change is additive and backward-compatible: a new role string, new
control-plane methods, and a null session id in the manage welcome. The estimated
hermes-agent diff is roughly 150 to 250 lines of Python with no core-session
rewrite. What the plan deliberately does not provide is true multi-user. Tokens
are bearer credentials with no per-user identity, ownership, quota, or revocation,
and a manage-created session spends the host's model budget, so the manage token
is an admin credential for a single trusted operator. Ownership, isolation, and
quotas across mutually distrustful users are a much larger change, either an ACL
layer in hermes core or a thin Rust supervisor in front of the share protocol,
and are deferred until a genuine multi-tenant need appears.
