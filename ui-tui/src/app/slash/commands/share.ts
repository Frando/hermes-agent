import type { ShareTicketsResponse } from '../../../gatewayTypes.js'
import type { SlashCommand } from '../types.js'

export const shareCommands: SlashCommand[] = [
  {
    help: 'Show the join tickets for sharing this session',
    name: 'share',
    run: (_arg, ctx) => {
      ctx.gateway
        .rpc<ShareTicketsResponse>('share.tickets', {})
        .then(
          ctx.guarded<ShareTicketsResponse>(res => {
            if (!res.sharing || !res.control) {
              ctx.transcript.sys(
                'This session is not being shared. Relaunch with `hermes share` to share it.'
              )

              return
            }

            ctx.transcript.sys(
              [
                'Sharing this session over iroh.',
                `  watch:   hermes join ${res.watch}`,
                `  control: hermes join ${res.control}`
              ].join('\n')
            )
          })
        )
        .catch(ctx.guardedErr)
    }
  }
]
