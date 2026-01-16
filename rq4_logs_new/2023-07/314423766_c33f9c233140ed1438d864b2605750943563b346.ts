/**
 * @ author: richen
 * @ copyright: Copyright (c) - <richenlin(at)gmail.com>
 * @ license: BSD (3-Clause)
 * @ version: 2020-12-15 11:49:15
 */

import { gRPCExceptionHandler } from "./handler/grpc";
import { httpExceptionHandler } from "./handler/http";
import { wsExceptionHandler } from "./handler/ws";
import { IOCContainer } from "koatty_container";

/**
 * Predefined runtime exception
 *
 * @export
 * @class HttpError
 * @extends {Error}
 */
export class Exception extends Error {
  public status: number;
  public code: number;
  readonly type = "Exception";

  /**
   * Creates an instance of Exception.
   * @param {string} message
   * @param {number} [code=1]
   * @param {number} [status]
   * @memberof Exception
   */
  constructor(message: string, code = 1, status = 0) {
    super(message);
    this.status = status;
    this.code = code;
  }

  /**
   * Default exception handler
   *
   * @param {KoattyContext} ctx
   * @returns {*}  
   * @memberof Exception
   */
  async default(ctx: any): Promise<any> {
    switch (ctx.protocol) {
      case "grpc":
        return gRPCExceptionHandler(ctx, this);
      case "ws":
      case "wss":
        return wsExceptionHandler(ctx, this);
      default:
        return httpExceptionHandler(ctx, this);
    }
  }
}

/**
 * Indicates that an decorated class is a "ExceptionHandler".
 * @ExceptionHandler()
 * 
 * export class BusinessException extends Exception { 
 *    constructor(message: string, code: number, status: number) { ... }
 *    handler(ctx: KoattyContext) { 
 * 
 *      ...//Handling business exceptions 
 * 
 *    }
 * }
 *
 * @export
 * @param {string} [identifier] class name
 * @returns {ClassDecorator}
 */
export function ExceptionHandler(): ClassDecorator {
  return (target: any) => {
    const identifier = IOCContainer.getIdentifier(target);
    // if (identifier === "Exception") {
    //     throw new Error("class name cannot be `Exception`");
    // }
    // if (!identifier.endsWith("Exception")) {
    //     throw Error("class name must end with 'Exception'");
    // }
    // if (!target.prototype.type) {
    //     throw new Error("class's property 'type' must be set");
    // }
    if (!(target.prototype instanceof Exception)) {
      throw new Error(`class ${identifier} does not inherit from class 'Exception'`);
    }
    IOCContainer.saveClass("COMPONENT", target, "ExceptionHandler");
  };
}