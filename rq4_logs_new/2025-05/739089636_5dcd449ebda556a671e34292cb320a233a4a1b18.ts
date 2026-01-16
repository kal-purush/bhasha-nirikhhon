import type { HttpContext } from "@adonisjs/core/http"
import type { NextFn } from "@adonisjs/core/types/http"

import logger from "@adonisjs/core/services/logger"
import { toNodeHandler } from "better-auth/node"

import { auth } from "#config/auth"
import env from "#start/env"

export default class BetterAuthMiddleware {
  private authHandler: ReturnType<typeof toNodeHandler>

  constructor() {
    this.authHandler = toNodeHandler(auth)
  }

  async handle(ctx: HttpContext, next: NextFn) {
    const { request, response } = ctx

    if (!request.url().startsWith("/auth")) {
      return next()
    }

    // Set CORS headers
    const httpHeaders = {
      "Access-Control-Allow-Origin": env.get("TRUSTED_ORIGIN_URL"),
      "Access-Control-Allow-Credentials": "true",
    }

    const headers = new Headers(httpHeaders)

    response.response.setHeaders(headers)

    console.log("BetterAuthMiddleware triggered", request.method(), request.url())

    try {
      logger.info("Better Auth request received", {
        url: request.url(),
        method: request.method(),
        contentType: request.header("content-type"),
      })

      const req = request.request
      req.url = request.request.url
      ;(req as any).originalUrl = request.url()

      await this.authHandler(req, response.response)
    } catch (error) {
      logger.error("Error in Better Auth middleware", error)

      try {
        return response.status(500).send({
          error: "Error processing Better Auth request",
          message: error.message,
        })
      } catch (e) {
        logger.error("Impossible to send error response", e)
      }
    }
  }
}