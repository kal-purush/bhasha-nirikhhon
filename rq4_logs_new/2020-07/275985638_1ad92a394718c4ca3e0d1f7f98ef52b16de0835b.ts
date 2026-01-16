import { Request, Response, NextFunction } from "express"

import { SessionRequestHandlerBase } from "~/src/server/gyst-server/express-server-endpoint-collection/endpoint-base/session"
import { setRedirectUrl } from "~/src/server/gyst-server/common/session"
import { cred_module_collection } from "~/src/server/cred-module-collection"

export class ErrorHandler extends SessionRequestHandlerBase {
  error:any
  storeParams() {}

  /**
   * 2020-07-04 03:33
   * 
   * `ExpressRequest` accepting `is_error_handler` then if true returning `ErrorRequestHandler`
   * from `handler` could be an option. However this ErrorHandler is used in only one place.
   * Not updating the `ExpressRequest` just for that for now.
   */
  handler() {
    return async (error:any, req:Request, res:Response, next:NextFunction) => {
      this.error = error
      this.req = req
      this.res = res
      return await this.handle()
    }
  }

  async doTasks():Promise<void> {
    /**
     * 2020-07-04 03:19
     * Store error to fix later.
     * 
     * Could store the error with id, and send the id to the response so that
     * the user only needs to share this id.
     */
    console.log(this.error)

    this.res.status(500).send({
      status: 500,
      name: "DEV_FAULT",
      message: "Developer forgot to handle this error. Sorry."
    })
  }
}