// created API router from Next.js; added handler API
// handler API will pass the router and context (which is invoked on every request)

import * as trpcNext from "@trpc/server/adapters/next";

import { serverRouter } from "@/server/router";
import { createContext } from "@/server/context";

export default trpcNext.createNextApiHandler({
  router: serverRouter,
  createContext,
});