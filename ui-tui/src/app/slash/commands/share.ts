import type { ShareTicketsResponse } from '../../../gatewayTypes.js'
import { patchUiState } from '../../uiStore.js'
import type { SlashCommand } from '../types.js'

export const shareCommands: SlashCommand[] = [
  {
    help: 'Share this session and show its join tickets',
    name: 'share',
    run: (_arg, ctx) => {
      // share.start mints this session's tokens and binds the shared endpoint on
      // first use. On success the gateway emits a share.info event (routed to the
      // host only), which renders the banner, so there is nothing to print here.
      ctx.gateway
        .rpc<ShareTicketsResponse>('share.start', { session_id: ctx.sid })
        .then(
          ctx.guarded<ShareTicketsResponse>(res => {
            if (!res.sharing) {
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
      ctx.gateway
        .rpc<ShareTicketsResponse>('share.stop', { session_id: ctx.sid })
        .then(
          ctx.guarded<ShareTicketsResponse>(res => {
            // Clear the banner so the self-heal effect drops it and stops
            // re-asserting it.
            patchUiState({ shareInfo: null })
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
