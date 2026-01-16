import { z } from "zod";

import {
  createTRPCRouter,
  publicProcedure,
  privateProcedure,
} from "~/server/api/trpc";

export const matchesRouter = createTRPCRouter({
  hello: publicProcedure
    .input(z.object({ text: z.string() }))
    .query(({ input }) => {
      return {
        greeting: `Hello ${input.text}`,
      };
    }),
  getAll: publicProcedure.query(({ ctx }) => {
    return ctx.prisma.match.findMany();
  }),
  getUserMatches: privateProcedure.query(async ({ ctx }) => {
    const userId = ctx.currentUser;
    const matches = await ctx.prisma.match.findMany({
      where: { userId },
      include: {
        homeTeam: {
          select: {
            name: true,
          },
        },
      },
    });

    return matches;
  }),
});