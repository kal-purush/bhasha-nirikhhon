import { z } from "zod";

import { createTRPCRouter, publicProcedure, protectedProcedure } from "../trpc";

export const itemRouter = createTRPCRouter({
    getMostRecent: publicProcedure.query(({ ctx }) => {
        const a = ctx.prisma.item.findMany(
            {
                orderBy: {
                    createdAt: 'desc'
                },
                take: 10,
                include: {
                    ItemLinks: {
                        include: {
                            link: true
                        }
                    }
                }
            }
        );

        return a
    }),
});