import * as HttpStatusCodes from "stoker/http-status-codes";

import { prisma } from "@/server/prisma/client";
import type {
  ListRoute,
  CreateRoute,
  UpdateRoute
} from "@/server/routes/payments/payments.routes";
import { AppRouteHandler } from "@/types/server";

type QueryParams = {
  page?: string;
  limit?: string;
  search?: string;
};

export const list: AppRouteHandler<ListRoute> = async (c) => {
  const user = c.get("user");
  const session = c.get("session");

  if (!user) {
    return c.json(
      { message: "Unauthenticated access" },
      HttpStatusCodes.UNAUTHORIZED
    );
  }

  if (!session?.activeOrganizationId) {
    return c.json(
      { message: "Organization not found" },
      HttpStatusCodes.NOT_FOUND
    );
  }

  const {
    page = "1",
    limit = "10",
    search = ""
  } = c.req.query() as QueryParams;

  // Convert to numbers and validate
  const pageNum = Math.max(1, parseInt(page));
  const limitNum = Math.max(1, Math.min(100, parseInt(limit))); // Cap at 100 items
  const offset = (pageNum - 1) * limitNum;

  // Build the where condition
  const whereCondition = {};

  // Add search condition if provided
  let paymentWhereCondition = {};

  if (search && search.trim() !== "") {
    paymentWhereCondition = {
      name: {
        contains: search,
        mode: "insensitive"
      }
    };
  }

  // Count query for total number of records
  const totalPayments = await prisma.payment.count({
    where: {
      ...whereCondition,
      ...(Object.keys(paymentWhereCondition).length > 0
        ? paymentWhereCondition
        : undefined)
    }
  });

  // Main query with pagination
  const payments = await prisma.payment.findMany({
    where: whereCondition,
    skip: offset,
    take: limitNum,
    orderBy: {
      createdAt: "desc"
    }
  });

  return c.json(
    {
      payments: payments,
      pagination: {
        total: totalPayments,
        page: pageNum,
        limit: limitNum,
        totalPages: Math.ceil(totalPayments / limitNum)
      }
    },
    HttpStatusCodes.OK
  );
};

export const create: AppRouteHandler<CreateRoute> = async (c) => {
  const user = c.get("user");
  const session = c.get("session");

  if (!user) {
    return c.json(
      { message: "Unauthenticated access" },
      HttpStatusCodes.UNAUTHORIZED
    );
  }

  if (!session?.activeOrganizationId) {
    return c.json(
      { message: "Organization not found" },
      HttpStatusCodes.FORBIDDEN
    );
  }

  const body = c.req.valid("json");

  const createdPayment = await prisma.payment.create({
    data: body
  });

  return c.json(createdPayment, HttpStatusCodes.OK);
};

export const update: AppRouteHandler<UpdateRoute> = async (c) => {
  const user = c.get("user");
  const session = c.get("session");

  if (!user) {
    return c.json(
      { message: "Unauthenticated access" },
      HttpStatusCodes.UNAUTHORIZED
    );
  }

  if (!session?.activeOrganizationId) {
    return c.json(
      { message: "Organization not found" },
      HttpStatusCodes.FORBIDDEN
    );
  }

  const body = c.req.valid("json");
  const params = c.req.valid("param");

  const updatedPayment = await prisma.payment.update({
    where: { id: params.id },
    data: body
  });

  return c.json(updatedPayment, HttpStatusCodes.OK);
};