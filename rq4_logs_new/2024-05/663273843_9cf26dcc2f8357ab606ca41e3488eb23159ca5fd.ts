import { TRPCError } from "@trpc/server";
import { env } from "process";

import { createTRPCRouter, publicProcedure } from "~/server/api/trpc";

import { seed, unSeed } from "~/server/db/seed";

export const seedRouter = createTRPCRouter({
  addRandomData: publicProcedure.mutation(() => {
    if (env.NODE_ENV === "production")
      throw new TRPCError({ code: "BAD_REQUEST" });
    return seed();
  }),
  clearCurrentData: publicProcedure.mutation(async () => {
    if (env.NODE_ENV === "production")
      throw new TRPCError({ code: "BAD_REQUEST" });

    return unSeed();
  }),
});