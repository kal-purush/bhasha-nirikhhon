"use server";
import { prisma } from "@/db/prisma";

export async function getLatestBookings() {
  try {
    const latestBookings = await prisma.booking.findMany({
      take: 10,
      orderBy: {
        createdAt: "desc",
      },
      include: {
        car: {
          select: {
            modelName: true,
            image: true,
            location: true,
          },
        },
      },
    });

    return latestBookings;
  } catch (error) {
    console.error("Error fetching latest bookings:", error);
    return null;
  }
}
