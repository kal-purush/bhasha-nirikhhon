"use server";
import { OrderStatus, RoleStatus } from "@prisma/client";
import { authAction } from "../next-safe-action";
import { authorizationMiddleware } from "../next-safe-action/middleware/auth";
import {
  changOrderStatusSchema,
  URL_PATHS,
} from "@/constants";
import { prisma } from "@/lib";
import { revalidatePath } from "next/cache";

export const changeStatusAction = authAction
  .use(authorizationMiddleware([RoleStatus.CHEF, RoleStatus.ADMIN,RoleStatus.DRIVER]))
  .schema(changOrderStatusSchema)
  .action(async ({ parsedInput, ctx }) => {
    const { id, status } = parsedInput;

    const order = await prisma.order.findUnique({ where: { id } });
    if (!order) {
      throw new Error("Order not found");
    }

    await prisma.order.update({
      where: { id },
      data: {
        status: status as OrderStatus,
      },
    });

    const { user } = ctx;
    revalidatePaths(user.role);

    return {
      message: `Order status changed to ${status}`,
    };
  });

const revalidatePaths = (userRole: RoleStatus) => {
  switch (userRole) {
    case RoleStatus.CHEF:
      revalidatePath(URL_PATHS.CHEF.ORDERS);

      break;

    case RoleStatus.DRIVER:
      revalidatePath(URL_PATHS.DRIVER);

      break;
  }
};