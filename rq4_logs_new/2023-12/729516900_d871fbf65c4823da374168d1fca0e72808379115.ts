import cors from "@koa/cors";
import Koa from "koa";
import bodyParser from "koa-bodyparser";
import compose from "koa-compose";
import { router } from "./routes";
import { logMiddleware } from "./middleware/logger";
import { errorMiddleware } from "./middleware/error";

export const createApp = <State, Context>(
  ...middleware: Array<Koa.Middleware<State, Context>>
) =>
  new Koa()
    .use(cors())
    .use(bodyParser())
    .use(compose(middleware))
    .use(bodyParser())
    .use(logMiddleware)
    .use(errorMiddleware)
    .use(router.routes())
    .use(router.allowedMethods());