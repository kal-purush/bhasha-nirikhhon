import { NotFoundException } from "nextlove"
import { z } from "zod"

import { withRouteSpec } from "lib/middleware/index.ts"

import { bridge_client_session } from "lib/zod/bridge_client_session.ts"

export default withRouteSpec({
  auth: ["bridge_client_session"],
  methods: ["POST"],
  jsonResponse: z.object({
    bridge_client_session,
  }),
} as const)(async (req, res) => {
  const { db, auth } = req

  const pairing_code_expires_at = new Date(
    new Date().getTime() + 3 * 60 * 1000,
  ).toISOString()

  const bridge_client_session = db.bridge_client_sessions.find(
    (bridge_client_session) =>
      bridge_client_session.bridge_client_session_id ===
      auth.bridge_client_session.bridge_client_session_id,
  )

  if (bridge_client_session == null) {
    throw new NotFoundException({
      type: "bridge_client_session_not_found",
      message: "Bridge client session not found",
    })
  }

  db.updateBridgeClientSession({
    bridge_client_session_id: bridge_client_session.bridge_client_session_id,
    pairing_code_expires_at,
  })

  res.status(200).json({
    bridge_client_session: {
      ...bridge_client_session,
      pairing_code_expires_at,
    },
  })
})