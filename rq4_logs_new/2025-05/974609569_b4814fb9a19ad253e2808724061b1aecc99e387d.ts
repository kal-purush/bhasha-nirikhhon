import * as HttpStatusCodes from "stoker/http-status-codes";

import { prisma } from "@/server/prisma/client";
import type {
  ListRoute,
  AssignRoute,
  CreateRoute

  // TODO: Complete these route handlers
  // FindOneRoute,
  // RemoveRoute,
  // UpdateRoute
} from "@/server/routes/badges/badges.routes";
import { AppRouteHandler } from "@/types/server";

type QueryParams = {
  page?: string;
  limit?: string;
  search?: string;
};

export const list: AppRouteHandler<ListRoute> = async (c) => {
  const user = c.get("user");

  if (!user) {
    return c.json(
      { message: "Unauthenticated access" },
      HttpStatusCodes.UNAUTHORIZED
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
  let badgeWhereCondition = {};

  if (search && search.trim() !== "") {
    badgeWhereCondition = {
      name: {
        contains: search,
        mode: "insensitive"
      }
    };
  }

  // Count query for total number of records
  const totalBadges = await prisma.badge.count({
    where: {
      ...whereCondition,
      ...(Object.keys(badgeWhereCondition).length > 0
        ? badgeWhereCondition
        : undefined)
    }
  });

  // Main query with pagination
  const badges = await prisma.badge.findMany({
    where: whereCondition,
    skip: offset,
    take: limitNum,
    orderBy: {
      createdAt: "desc"
    }
  });

  // Filter out records where user doesn't match search criteria
  const filteredBadges = search
    ? badges.filter((badge) =>
        badge?.name?.toLowerCase().includes(search.toLowerCase())
      )
    : badges;

  return c.json(
    {
      badges: filteredBadges,
      pagination: {
        total: totalBadges,
        page: pageNum,
        limit: limitNum,
        totalPages: Math.ceil(totalBadges / limitNum)
      }
    },
    HttpStatusCodes.OK
  );
};

export const create: AppRouteHandler<CreateRoute> = async (c) => {
  const user = c.get("user");

  if (!user) {
    return c.json(
      { message: "Unauthenticated access" },
      HttpStatusCodes.UNAUTHORIZED
    );
  }

  const body = c.req.valid("json");

  const createdBadge = await prisma.badge.create({
    data: body
  });

  return c.json(createdBadge, HttpStatusCodes.OK);
};

export const assign: AppRouteHandler<AssignRoute> = async (c) => {
  const user = c.get("user");

  if (!user) {
    return c.json(
      { message: "Unauthenticated access" },
      HttpStatusCodes.UNAUTHORIZED
    );
  }

  const body = c.req.valid("json");
  const params = c.req.valid("param");

  await prisma.childBadge.create({
    data: { childId: params.id, badgeId: body.badgeId }
  });

  return c.json({ message: `Badge assigned to child !` }, 200);
};