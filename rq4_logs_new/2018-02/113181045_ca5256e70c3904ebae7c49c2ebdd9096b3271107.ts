import * as debug from "debug";
import { IDebugger } from "debug";

/**
 * Contains the loggers/debuggers used in different contexts.
 */
export class Log {
   public static readonly SERVER: IDebugger = Log.newLogger("server");
   public static readonly APP: IDebugger = Log.newLogger("app");
   public static readonly DB: IDebugger = Log.newLogger("database");

   /**
    * @param name The namespace of the logger
    * @param enabled Whether the logger should be enabled from the start
    */
   private static newLogger(name: string, enabled?: boolean): debug.IDebugger {
      if (enabled === undefined || enabled === true) {
         debug.enable(name);
      }
      return debug(name);
   }
}