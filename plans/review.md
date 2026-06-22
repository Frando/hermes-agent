# iroh session-sharing — production-readiness review

Branch: `new-tui` (pushed as `Frando/iroh-share`). Scope: the iroh share feature
(`tui_gateway/iroh_share.py`, `server.py`, `transport.py`, `entry.py`,
`hermes_cli/main.py`, the `ui-tui/src` share changes). Reviewed end-to-end for
reliability, production-readiness, error logging, and token-validation integrity.

Bottom line: the **token/auth boundary is sound** — a peer cannot reach
`server.dispatch` without a valid token (`secrets.compare_digest`, role bound to
the token, grab re-checks role), watchers are limited to a tiny audited
read-only allow-list, every other method fails closed, and each joiner is pinned
to one session. The fixes below close a control-ticket leak and several logging
gaps; the open items are reliability/correctness, not auth bypass.

---

## Fixed in this pass (committed)

- **[SECURITY] `share.info` control ticket was broadcast through the fan-out.**
  `entry.py` emitted the startup tickets via `write_json`, which resolves to the
  session fan-out — so the secret **control** ticket would reach every attached
  joiner. Safe only by boot-time timing (no joiner attached yet), not
  structurally. Now written to the fan-out's *primary* (the host's own stdout)
  only, so it can never reach a joiner regardless of attach timing.
  `tui_gateway/entry.py`.
- **[SECURITY/defense-in-depth] `share.tickets` returned the control ticket to
  anyone.** It is denied to non-controller joiners by the acceptor allow-list,
  but a controller-joiner could read it. Now the control ticket is returned only
  when the request comes from the host transport (`current_transport() is
  _stdio_transport`); joiners get `watch` only. Locked in by
  `test_share_tickets_omits_control_for_non_host`. `tui_gateway/server.py`.
- **[RELIABILITY] iroh endpoint leaked on exit.** `IrohShareHost.stop()` existed
  but was never called. Registered an `atexit` hook to close the endpoint
  cleanly (relay drops our mapping, joiners torn down). `tui_gateway/entry.py`.
- **[LOGGING] Silenced failures now logged** (user requirement): fan-out setup
  failure (`entry.py`, was a bare `return`), fan-out member write *exceptions*
  vs. clean disconnects (`transport.py`, `write`/`write_to_others`), session
  DB-row failure that breaks joiner resume (`server.py`, debug→warning), and the
  acceptor's `accept_next`/handshake failures (`iroh_share.py`, debug diagnostics).

---

## Open findings

### HIGH — control arbitration is bypassable by the host

Only `prompt.submit` consults `control_denied(current_transport())`
(`server.py` ~6346). The other agent-driving methods do **not**: `slash.exec`,
`session.steer`, `prompt.background`, `preview.restart`, and the server-side
`command.dispatch` `steer`/`goal` branches.

- For **joiners** this is not exploitable: those methods aren't in
  `_READ_ONLY_METHODS`, so the acceptor denies them to any non-controller.
- For the **host** it is a real gap: host requests bypass the acceptor, so while
  a joiner holds control (after `/grab`), the host can still drive the agent via
  `/steer`, a slash command, etc. That defeats the "only one party drives at a
  time" guarantee the feature is built around.

**Fix:** extract the existing `prompt.submit` gate into a helper and apply it to
each agent-driving handler:

```python
def _deny_if_not_controller():
    share = _iroh_share_host
    if share is not None:
        return share.control_denied(current_transport())  # None when allowed
    return None
```

It is safe to add broadly: `control_denied` returns `None` when not sharing or
when the caller already holds control, so non-shared gateways and the
controlling party are unaffected. Add a test per method (host-blocked-while-
joiner-controls, controller-allowed).

### MEDIUM — shared session selection can drift / fragment

`active_shared_session_id()` (`server.py:688`) returns the *most recently
active* live session, recomputed per connection. Consequences:
- Two joiners connecting at different times can pin to *different* sessions.
- If the host switches focus to another session, a later joiner shares that one
  — possibly a session the host didn't intend to expose.
- The `>=` tie-break picks the last in dict order (nondeterministic-ish).

Not an auth bypass (each joiner is still pinned and gated), but a mis-share /
fragmentation risk. **Fix:** pin the shared session id once (at acceptor start or
at first joiner) and reuse it for all joiners; or let the host explicitly
designate the shared session. Handle the "share started before any session
exists" case (resolve lazily, then freeze).

### MEDIUM — share banner thrashes in long shared sessions (TUI)

`capHistory` (`ui-tui/src/app/useMainApp.ts`, cap = `MAX_HISTORY` 800) preserves
only the intro row, not the `kind:'share'` banner. Past 800 transcript rows the
banner is pruned on each append, and the self-heal effect re-appends a *new*
banner object at the bottom — new `messageId`, so React remounts it and it jumps
around. (The effect is otherwise loop-free — confirmed.) **Fix:** treat the
banner like the intro — preserve a `kind:'share'` row in `capHistory`, or render
it out-of-band (as the intro/panel are rendered in `appLayout.tsx`) instead of as
a capped transcript item.

### LOW

- **`_MAX_CLIENTS` TOCTOU** (`iroh_share.py` ~392): the count is checked before
  `await`s and the client is added after, so concurrent handshakes can overshoot
  the 32 cap. Bounded DoS only. Track an in-flight counter or re-check after add.
- **No rate-limit/lockout on token attempts.** A party that knows the endpoint
  id (e.g. a watcher) could brute-force the control token by reconnecting. The
  96-bit token + `compare_digest` makes this infeasible, so it's acceptable, but
  note there is no connection throttling.
- **`/grab` reply typed as `{output?: string}`** (`ui-tui/.../slash/commands/
  share.ts`) bypasses `asCommandDispatch`; only the `exec` variant is handled.
  Works today (grab is exec-only) but is a latent contract trap.
- **`empty` flag flips false** once the banner is asserted
  (`useMainApp.ts`), suppressing the composer placeholder at startup of a shared
  session. Cosmetic; exclude `kind:'share'` from the `empty` predicate if unwanted.
- **`share.ts` `.catch(ctx.guardedErr)` is dead** (rpc never rejects). Harmless,
  but the friendly "not being shared" message only fires when the gateway
  implements `share.tickets`; a build lacking it shows a raw method-not-found.
- **`FanoutTransport._closed` is dead weight** (`transport.py`): set by `close()`
  but never read by `write`/`add`. Either honour it (reject `add` after close) or
  drop it; document that close is a no-op for the shared stdio fan-out.

---

## Verified sound (no change needed)

- Token validation: no path to `dispatch` without a valid token; role bound to
  token; `_grab` re-checks `ROLE_CONTROL`; watcher escalation impossible.
- Session pinning: requests naming any session other than the pinned
  `(ephemeral_id, resume_key)` are refused (4031).
- `FanoutTransport` concurrency: snapshot-under-lock iteration, no
  mutate-during-iteration, lock never held across blocking I/O, single-shot
  global swap before concurrency starts.
- Per-session frame filter (`_frame_for_sid`): a joiner never sees the host's
  other sessions.
- CLI: `cmd_join` validates tickets, exits non-zero with a clean message, no
  hang/traceback; `share`/`join` registered in `_BUILTIN_SUBCOMMANDS`.
- Boot thread never delays `gateway.ready`; late `share.info` is handled by the
  TUI's persisted `shareInfo` state.
- Tickets are never written to disk; the start-failure path is logged + surfaced.
- TS: typecheck + eslint clean; events session-scoped; `message.user` not echoed
  to the submitter.
</content>
</invoke>
