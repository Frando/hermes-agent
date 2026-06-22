import type { ShareTicketsResponse } from '../../../gatewayTypes.js'
import type { SlashCommand } from '../types.js'

// Set by `hermes join <ticket>`: the ticket this TUI joined with. Its presence
// means this is a joined (remote) session, which cannot itself be reshared or
// unshared — only the host controls that.
const joinTicket = (): string => (process.env.HERMES_TUI_JOIN_TICKET ?? '').trim()

const ticketLines = (watch: string, control: string): string =>
  [
    'Sharing this session over iroh.',
    `  watch:   hermes join ${watch}`,
    `  control: hermes join ${control}`
  ].join('\n')

export const shareCommands: SlashCommand[] = [
  {
    help: 'Share this session and show its join tickets',
    name: 'share',
    run: (_arg, ctx) => {
      const joined = joinTicket()

      if (joined) {
        ctx.transcript.sys(
          `You joined this session, so you can't reshare it.\n  joined with: hermes join ${joined}`
        )

        return
      }

      // share.start is idempotent: the first call mints this session's tokens
      // and binds the shared endpoint; later calls return the same tickets. Print
      // them on every call so re-running /share reshows the join commands.
      ctx.gateway
        .rpc<ShareTicketsResponse>('share.start', { session_id: ctx.sid })
        .then(
          ctx.guarded<ShareTicketsResponse>(res => {
            if (res.sharing && res.watch && res.control) {
              ctx.transcript.sys(ticketLines(res.watch, res.control))
            } else {
              ctx.transcript.sys('Could not share this session.')
            }
          })
        )
        .catch(ctx.guardedErr)
    }
  },
  {
    help: 'Stop sharing this session',
    name: 'unshare',
    run: (_arg, ctx) => {
      if (joinTicket()) {
        ctx.transcript.sys('You joined this session; only the host can stop sharing it.')

        return
      }

      ctx.gateway
        .rpc<ShareTicketsResponse>('share.stop', { session_id: ctx.sid })
        .then(
          ctx.guarded<ShareTicketsResponse>(res => {
            ctx.transcript.sys(
              res.was_sharing
                ? 'Stopped sharing this session.'
                : 'This session was not being shared.'
            )
          })
        )
        .catch(ctx.guardedErr)
    }
  },
  {
    help: 'Take control of a shared session',
    name: 'grab',
    run: (_arg, ctx) => {
      // Routes via command.dispatch: a joiner's grab is intercepted by the iroh
      // acceptor; the host's reaches the gateway. Both reply with an exec line.
      ctx.gateway
        .rpc<{ output?: string }>('command.dispatch', { name: 'grab', session_id: ctx.sid })
        .then(
          ctx.guarded<{ output?: string }>(res => {
            ctx.transcript.sys(res.output || 'grab: no response')
          })
        )
        .catch(ctx.guardedErr)
    }
  }
]
