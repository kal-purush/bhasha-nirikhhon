import * as HttpStatusCodes from "stoker/http-status-codes";

import { prisma } from "@/server/prisma/client";
import type {
  GetRoute,
  SendRoute,
  CreateTagRoute,
  GetTagsRoute
} from "@/server/routes/notifications/notifications.routes";
import { AppRouteHandler } from "@/types/server";

export const get: AppRouteHandler<GetRoute> = async (c) => {
  const user = c.get("user");

  if (!user) {
    return c.json(
      { message: "Unauthenticated access" },
      HttpStatusCodes.UNAUTHORIZED
    );
  }

  const notifications = await prisma.notification.findMany({
    where: {
      recipients: {
        some: {
          recipientId: user.id
        }
      }
    }
  });

  return c.json(notifications, HttpStatusCodes.OK);
};

export const send: AppRouteHandler<SendRoute> = async (c) => {
  const user = c.get("user");

  if (!user) {
    return c.json(
      { message: "Unauthenticated access" },
      HttpStatusCodes.UNAUTHORIZED
    );
  }

  const body = c.req.valid("json");

  // Create the notification
  const notification = await prisma.notification.create({
    data: {
      content: body.content,
      senderId: user.id
    }
  });

  // Assign tags to the notification
  await prisma.notificationTag_Notification.createMany({
    data: body.tags.map((tagId: string) => ({
      notificationId: notification.id,
      notificationTagId: tagId
    }))
  });

  // Send the notification to each recipient
  await prisma.notificationRecipient.createMany({
    data: body.recipients.map((recipientId: string) => ({
      notificationId: notification.id,
      recipientId
    }))
  });

  return c.json(notification, HttpStatusCodes.OK);
};

// ------------ Get Tags Handler ------------
export const getTags: AppRouteHandler<GetTagsRoute> = async (c) => {
  const user = c.get("user");

  if (!user) {
    return c.json(
      { message: "Unauthenticated access" },
      HttpStatusCodes.UNAUTHORIZED
    );
  }

  const tags = await prisma.notificationTag.findMany();

  return c.json(tags, HttpStatusCodes.OK);
};

// ------------ Create Tag Handler ------------
export const createTag: AppRouteHandler<CreateTagRoute> = async (c) => {
  const user = c.get("user");

  if (!user) {
    return c.json(
      { message: "Unauthenticated access" },
      HttpStatusCodes.UNAUTHORIZED
    );
  }

  const body = c.req.valid("json");

  const createTag = await prisma.notificationTag.create({
    data: { name: body.name }
  });

  return c.json(createTag, HttpStatusCodes.OK);
};