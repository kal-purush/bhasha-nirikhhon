import { NotFoundException } from "nextlove"
import { z } from "zod"
import { Temporal } from "@js-temporal/polyfill"

import { withRouteSpec } from "lib/middleware/with-route-spec.ts"
import { randomUUID } from "crypto"

export default withRouteSpec({
  auth: "none",
  methods: ["POST"],
  jsonBody: z.object({
    pairing_code: z.string(),
  }),
  jsonResponse: z.object({}),
} as const)(async (req, res) => {
  const { pairing_code } = req.body

  const bridge_client_session = req.db.bridge_client_sessions.find(
    (bridge_client_session) => bridge_client_session.pairing_code === pairing_code,
  )

  if (bridge_client_session == null) {
    throw new NotFoundException({
      type: "bridge_client_session_not_foud",
      message: "Bridge client session not found",
    })
  }

  bridge_client_session.tailscale_auth_key = `ts-auth-${randomUUID()}`
  bridge_client_session._ext_tailscale_auth_key_id = `ts-id-${randomUUID()}`

  const now = Temporal.Now.plainDateTimeISO()
  bridge_client_session._tailscale_auth_key_expires_at = now.add({ weeks: 1 }).toString()

  res.status(200).json({})
})