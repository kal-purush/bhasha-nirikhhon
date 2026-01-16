import * as HttpStatusCodes from "stoker/http-status-codes";

import { prisma } from "@/server/prisma/client";
import type {
  ListRoute,
  CreateRoute,
  FindOneRoute,
  UpdateRoute,
  RemoveRoute
} from "@/server/routes/children/children.routes";
import { AppRouteHandler } from "@/types/server";

type QueryParams = {
  page?: string;
  limit?: string;
  search?: string;
};

// ------------ List all children ------------
export const list: AppRouteHandler<ListRoute> = async (c) => {
  const user = c.get("user");
  const session = c.get("session");

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

  // Add condition
  let childWhereCondition = {};

  if (search && search.trim() !== "") {
    childWhereCondition = {
      name: {
        contains: search,
        mode: "insensitive"
      }
    };
  }

  /**
   * Get Children details, depending on organization, role etc.
   * - If system user role is admin, all children records must be fetched
   * - If system user role is user,
   * -- We need to check user's active org role is
   * --- owner (nursery admin) / admin (teacher)
   * ---- then fetch all children records of that organization
   * --- if active org role = member
   * ---- then fetch all children records associated with system user's children array
   * -- If not active organization,
   * --- then fetch all children by parent id (user id)
   */
  if (user.role !== "admin" && session?.activeOrganizationId) {
    const organizationMember = await prisma.member.findFirst({
      where: { userId: user.id, organizationId: session.activeOrganizationId }
    });

    if (organizationMember?.role === "member") {
      childWhereCondition = { parentId: user.id };
    } else {
      childWhereCondition = { nurseryId: session.activeOrganizationId };
    }
  } else if (!session?.activeOrganizationId) {
    childWhereCondition = { parentId: user.id };
  }

  // Count query for total number of records
  const totalChildren = await prisma.child.count({
    where: childWhereCondition
  });

  // Main query with pagination
  const children = await prisma.child.findMany({
    where: childWhereCondition,
    // include: {
    //   parent: true
    // },
    skip: offset,
    take: limitNum,
    orderBy: {
      createdAt: "desc"
    }
  });

  // Filter out records where user doesn't match search criteria
  const filteredChildren = search
    ? children.filter(
        (child) =>
          child.firstName?.toLowerCase().includes(search.toLowerCase()) ||
          child.lastName?.toLowerCase().includes(search.toLowerCase())
      )
    : children;

  return c.json(
    {
      children: filteredChildren,
      pagination: {
        total: totalChildren,
        page: pageNum,
        limit: limitNum,
        totalPages: Math.ceil(totalChildren / limitNum)
      }
    },
    HttpStatusCodes.OK
  );
};

// ------------ Create Children ------------
export const create: AppRouteHandler<CreateRoute> = async (c) => {
  const user = c.get("user");

  if (!user) {
    return c.json(
      { message: "Unauthenticated access" },
      HttpStatusCodes.UNAUTHORIZED
    );
  }

  const childData = c.req.valid("json");

  const createdChild = await prisma.child.create({
    data: childData
  });

  return c.json(createdChild, HttpStatusCodes.OK);
};

// ------------ Find one children ------------
export const findOne: AppRouteHandler<FindOneRoute> = async (c) => {
  const user = c.get("user");

  if (!user) {
    return c.json(
      { message: "Unauthenticated access" },
      HttpStatusCodes.UNAUTHORIZED
    );
  }

  const params = c.req.valid("param");

  const child = await prisma.child.findUnique({
    where: { id: params.id }
  });

  if (!child) {
    return c.json({ message: "Child not found" }, HttpStatusCodes.NOT_FOUND);
  }

  if (user.role !== "admin" && child.parentId !== user.id) {
    const session = c.get("session");

    if (!session?.activeOrganizationId) {
      return c.json({ message: "Access Forbidden" }, HttpStatusCodes.FORBIDDEN);
    }

    const organizationMember = await prisma.member.findFirst({
      where: { userId: user.id, organizationId: session?.activeOrganizationId }
    });

    if (organizationMember?.role === "member") {
      return c.json({ message: "Access forbidden" }, HttpStatusCodes.FORBIDDEN);
    }
  }

  return c.json(child, HttpStatusCodes.OK);
};

// ------------ Update child ------------
export const update: AppRouteHandler<UpdateRoute> = async (c) => {
  const user = c.get("user");

  if (!user) {
    return c.json(
      { message: "Unauthenticated access" },
      HttpStatusCodes.UNAUTHORIZED
    );
  }

  const params = c.req.valid("param");
  const childData = c.req.valid("json");

  const updatedChild = await prisma.child.update({
    where: { id: params.id },
    data: childData
  });

  return c.json(updatedChild, HttpStatusCodes.OK);
};

// ------------ Delete child ------------
export const remove: AppRouteHandler<RemoveRoute> = async (c) => {
  const user = c.get("user");

  if (!user) {
    return c.json(
      { message: "Unauthenticated access" },
      HttpStatusCodes.UNAUTHORIZED
    );
  }

  const params = c.req.valid("param");

  const childExists = await prisma.child.findUnique({
    where: { id: params.id }
  });

  if (!childExists) {
    return c.json({ message: "Child not found" }, HttpStatusCodes.NOT_FOUND);
  }

  await prisma.child.delete({
    where: { id: params.id }
  });

  return c.json({ message: "Child deleted successfully" }, HttpStatusCodes.OK);
};