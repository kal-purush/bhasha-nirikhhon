import prisma from "@/lib/prisma";

export const resolvers = {
  Query: {
    comments: (parent: any, args: any, contextValue: any, info: any) => {
      return prisma.comment.findMany({
        where: { movieId: { equals: args.movieId } },
      });
    },
  },
  Mutation: {
    createComment: (parent: any, args: any, contextValue: any) => {
      return prisma.comment.create({ data: args });
    },
    deleteComment: (parent: any, args: any, contextValue: any) => {
      return prisma.comment.delete({ where: { id: args.id } });
    },
    updateComment: (parent: any, args: any, contextValue: any) => {
      return prisma.comment.update({
        where: { id: args.id },
        data: args.data,
      });
    },
  },
};