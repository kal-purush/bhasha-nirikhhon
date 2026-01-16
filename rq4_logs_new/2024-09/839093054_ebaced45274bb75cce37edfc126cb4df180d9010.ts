import { z } from "zod";

import {
  createTRPCRouter,
  protectedProcedure,
  publicProcedure,
} from "~/server/api/trpc";

export const newUser = createTRPCRouter({
  create: publicProcedure
    .input(z.object({ 
      name: z.string(),
      email: z.string(),
      password: z.string(),
    }))
    .mutation(async ({ ctx, input }) => {
      return ctx.db.user.create({
        data: {
          name: input.name,
          email: input.email,
          password: input.password,
        },
      });
    }),
  
  getUser: publicProcedure
    .input(z.object({
      name: z.string(),
    }))
    .query(async ({ ctx, input }) => {
      const user = await ctx.db.user.findFirst({
          where: { email: input.name },
        })
        if (user) {
          return user;
        }
      return false;
    }),

  });