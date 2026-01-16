import { z } from 'zod';
import { router, procedure } from '../trpc';

export const postRouter = router({
  all: procedure.query(async ({ ctx }) => {
    return await ctx.prisma.post.findMany();
  }),
  read: procedure
    .input(z.string().uuid())
    .query(async ({ ctx, input: postId }) => {
      return await ctx.prisma.post.findFirst({
        where: {
          id: postId
        }
      });
    }),
  create: procedure
    .input(
      z.object({
        title: z.string(),
        content: z.string()
      })
    )
    .mutation(async ({ ctx, input }) => {
      if (ctx.session === null) {
        return null;
      }
      try {
        const { id } = await ctx.prisma.post.create({
          data: {
            title: input.title,
            content: input.content,
            authorId: ctx.session.userId
          },
          select: {
            id: true
          }
        });
        return id;
      } catch {
        return null;
      }
    })
});