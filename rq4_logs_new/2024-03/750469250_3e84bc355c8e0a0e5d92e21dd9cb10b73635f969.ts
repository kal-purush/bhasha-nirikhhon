"use server";

import { getServerSession } from "next-auth";
import { db } from "../lib/prisma";
import { authOptions } from "../lib/auth";
import { redirect } from "next/navigation";

export const getBookings = async () => {
  const session = await getServerSession(authOptions);
  if (!session?.user) {
    return redirect("/");
  }

  const [confirmedBookings, finishedBookings] = await Promise.all([
    db.booking.findMany({
      where: {
        userId: (session.user as any).id,
        date: {
          gte: new Date(),
        },
      },
      include: {
        service: true,
        establishment: true,
        employee: true,
        user: true,
      },
    }),
    db.booking.findMany({
      where: {
        userId: (session.user as any).id,
        date: {
          lt: new Date(),
        },
      },
      include: {
        service: true,
        establishment: true,
        employee: true,
        user: true,
      },
    }),
  ]);

  return {
    confirmedBookings,
    finishedBookings,
  };
};