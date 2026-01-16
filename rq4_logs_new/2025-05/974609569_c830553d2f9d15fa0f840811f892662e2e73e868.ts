import * as HttpStatusCodes from "stoker/http-status-codes";

import { prisma } from "@/server/prisma/client";
import type {
  ListRoute,
  CreateRoute,
  FindOneRoute,
  UpdateRoute,
  RemoveRoute,
} from "./lessonPlans.routes";
import { AppRouteHandler } from "@/types/server";

type QueryParams = {
  page?: string;
  limit?: string;
  search?: string;
  classId?: string;
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
    search = "",
    classId,
  } = c.req.query() as QueryParams;

  // Convert to numbers and validate
  const pageNum = Math.max(1, parseInt(page));
  const limitNum = Math.max(1, Math.min(100, parseInt(limit))); // Cap at 100 items
  const offset = (pageNum - 1) * limitNum;

  // Build the where condition
  const whereCondition: any = {};

  // Add classId filter if provided
  if (classId) {
    whereCondition.classId = classId;
  }

  // Count query for total number of records
  const totalLessonPlans = await prisma.lessonPlan.count({
    where: whereCondition,
  });

  // Main query with pagination
  const lessonPlans = await prisma.lessonPlan.findMany({
    where: whereCondition,
    include: {
      teacher: {
        select: {
          id: true,
          user: {
            select: {
              name: true,
            },
          },
        },
      },
      class: {
        select: {
          id: true,
          name: true,
        },
      },
    },
    skip: offset,
    take: limitNum,
    orderBy: {
      createdAt: "desc",
    },
  });

  // Filter by search term if provided
  const filteredLessonPlans = search
    ? lessonPlans.filter(
        (plan) =>
          plan.title.toLowerCase().includes(search.toLowerCase()) ||
          plan.description.toLowerCase().includes(search.toLowerCase())
      )
    : lessonPlans;

  return c.json(
    {
      lessonPlans: filteredLessonPlans,
      pagination: {
        total: totalLessonPlans,
        page: pageNum,
        limit: limitNum,
        totalPages: Math.ceil(totalLessonPlans / limitNum),
      },
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

  try {
    // Find the member record for this user
    const member = await prisma.member.findFirst({
      where: {
        userId: user.id,
        role: { in: ["ADMIN", "TEACHER"] }, // Make sure user is an admin or teacher
      },
    });

    if (!member) {
      return c.json(
        { message: "Not authorized to create lesson plans" },
        HttpStatusCodes.FORBIDDEN
      );
    }

    // Create the lesson plan
    const lessonPlan = await prisma.lessonPlan.create({
      data: {
        ...body,
        teacherId: member.id,
      },
    });

    return c.json(lessonPlan, HttpStatusCodes.OK);
  } catch (error) {
    console.error("Error creating lesson plan:", error);
    return c.json(
      { message: "Failed to create lesson plan" },
      HttpStatusCodes.INTERNAL_SERVER_ERROR
    );
  }
};

export const findOne: AppRouteHandler<FindOneRoute> = async (c) => {
  const user = c.get("user");

  if (!user) {
    return c.json(
      { message: "Unauthenticated access" },
      HttpStatusCodes.UNAUTHORIZED
    );
  }

  const { id } = c.req.valid("param");

  try {
    const lessonPlan = await prisma.lessonPlan.findUnique({
      where: { id },
      include: {
        teacher: {
          select: {
            id: true,
            user: {
              select: {
                name: true,
              },
            },
          },
        },
        class: {
          select: {
            id: true,
            name: true,
          },
        },
      },
    });

    if (!lessonPlan) {
      return c.json(
        { message: "Lesson plan not found" },
        HttpStatusCodes.NOT_FOUND
      );
    }

    return c.json(lessonPlan, HttpStatusCodes.OK);
  } catch (error) {
    console.error("Error fetching lesson plan:", error);
    return c.json(
      { message: "Failed to retrieve lesson plan" },
      HttpStatusCodes.INTERNAL_SERVER_ERROR
    );
  }
};

export const update: AppRouteHandler<UpdateRoute> = async (c) => {
  const user = c.get("user");

  if (!user) {
    return c.json(
      { message: "Unauthenticated access" },
      HttpStatusCodes.UNAUTHORIZED
    );
  }

  const { id } = c.req.valid("param");
  const updates = c.req.valid("json");

  try {
    // Find the existing lesson plan
    const existingLessonPlan = await prisma.lessonPlan.findUnique({
      where: { id },
      include: {
        teacher: true,
      },
    });

    if (!existingLessonPlan) {
      return c.json(
        { message: "Lesson plan not found" },
        HttpStatusCodes.NOT_FOUND
      );
    }

    // Check if user is the creator or an admin
    const member = await prisma.member.findFirst({
      where: {
        userId: user.id,
      },
    });

    if (!member) {
      return c.json(
        { message: "Not a member of any organization" },
        HttpStatusCodes.FORBIDDEN
      );
    }

    const isTeacher = existingLessonPlan.teacherId === member.id;
    const isAdmin = member.role === "ADMIN";

    if (!isTeacher && !isAdmin) {
      return c.json(
        { message: "Not authorized to update this lesson plan" },
        HttpStatusCodes.FORBIDDEN
      );
    }

    // Update the lesson plan
    const updatedLessonPlan = await prisma.lessonPlan.update({
      where: { id },
      data: updates,
    });

    return c.json(updatedLessonPlan, HttpStatusCodes.OK);
  } catch (error) {
    console.error("Error updating lesson plan:", error);
    return c.json(
      { message: "Failed to update lesson plan" },
      HttpStatusCodes.INTERNAL_SERVER_ERROR
    );
  }
};

export const remove: AppRouteHandler<RemoveRoute> = async (c) => {
  const user = c.get("user");

  if (!user) {
    return c.json(
      { message: "Unauthenticated access" },
      HttpStatusCodes.UNAUTHORIZED
    );
  }

  const { id } = c.req.valid("param");

  try {
    // Find the existing lesson plan
    const existingLessonPlan = await prisma.lessonPlan.findUnique({
      where: { id },
      include: {
        teacher: true,
      },
    });

    if (!existingLessonPlan) {
      return c.json(
        { message: "Lesson plan not found" },
        HttpStatusCodes.NOT_FOUND
      );
    }

    // Check if user is the creator or an admin
    const member = await prisma.member.findFirst({
      where: {
        userId: user.id,
      },
    });

    if (!member) {
      return c.json(
        { message: "Not a member of any organization" },
        HttpStatusCodes.FORBIDDEN
      );
    }

    const isTeacher = existingLessonPlan.teacherId === member.id;
    const isAdmin = member.role === "ADMIN";

    if (!isTeacher && !isAdmin) {
      return c.json(
        { message: "Not authorized to delete this lesson plan" },
        HttpStatusCodes.FORBIDDEN
      );
    }

    // Delete the lesson plan
    await prisma.lessonPlan.delete({
      where: { id },
    });

    return c.json(
      { message: "Lesson plan deleted successfully" },
      HttpStatusCodes.OK
    );
  } catch (error) {
    console.error("Error deleting lesson plan:", error);
    return c.json(
      { message: "Failed to delete lesson plan" },
      HttpStatusCodes.INTERNAL_SERVER_ERROR
    );
  }
};