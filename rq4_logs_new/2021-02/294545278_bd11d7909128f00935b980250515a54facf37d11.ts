import Joi from "joi";
import { extendType, objectType, stringArg } from "nexus";

import { isAuthenticated } from "../rules";

export const UserStatus = objectType({
  name: "UserStatus",
  definition(t) {
    t.model.createdAt();
    t.model.emoji();
    t.model.id();
    t.model.message();
    t.model.updatedAt();
    t.model.user();
  },
});

export const UserStatusMutation = extendType({
  type: "Mutation",
  definition(t) {
    t.field("userSetStatus", {
      type: UserStatus,
      shield: isAuthenticated(),
      args: {
        emoji: stringArg(),
        message: stringArg(),
      },
      argSchema: Joi.object({
        emoji: Joi.string().allow(""),
        message: Joi.string().allow("").max(80).trim(),
      }),
      resolve: async (_root, { emoji, message }, ctx) => {
        const { status } = await ctx.prisma.user.update({
          where: { id: ctx.user?.id },
          select: { status: true },
          data: {
            status: {
              upsert: {
                create: { emoji, message },
                update: { emoji, message },
              },
            },
          },
        });
        return status;
      },
    });

    t.field("userClearStatus", {
      type: UserStatus,
      shield: isAuthenticated(),
      resolve: async (_root, _args, ctx) => {
        const userWithStatus = await ctx.prisma.user.findUnique({
          where: { id: ctx.user?.id },
          include: { status: true },
        });
        const status = userWithStatus?.status;
        if (!status) {
          return null;
        }
        return await ctx.prisma.userStatus.delete({ where: { id: status.id } });
      },
    });
  },
});