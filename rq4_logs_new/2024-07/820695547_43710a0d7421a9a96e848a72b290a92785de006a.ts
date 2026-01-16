import { z } from "zod";
import { db } from "../../db";

import {
  createTRPCRouter,
  protectedProcedure,
  publicProcedure,
} from "~/server/api/trpc";

export const eventsRouter = createTRPCRouter({

  createEvent: protectedProcedure
    .input(z.object({
        // id: z.number(),
        title:z.string(),
        content:z.string(),
        date:z.string(),
      }))
    .mutation(({input,ctx}) => {
        const event = db.event.create({
            data:{
                title: input.title,
                content: input.content,
                date: input.date,
                userId: ctx.session.user.id
            }
          });
    return event;
    }),

  getEvents: publicProcedure
      .query(async () => {
        const event = await db.event.findMany();
        return event;
      })
      
      
      // .query( async ({ctx}) => {
      //   const events = await db.event.findMany({
      //     where: {
      //       userId: ctx.session?.user.id
      //     }
      //   })
      // })

    
});