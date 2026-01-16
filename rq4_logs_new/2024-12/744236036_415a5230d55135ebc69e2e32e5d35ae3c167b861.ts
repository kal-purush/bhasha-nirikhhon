import { NextResponse } from "next/server";
import { prisma } from "@/db/prisma";

export async function GET() {
  try {
    const bookings = await prisma.booking.findMany({
      include: {
        car: {
          select: {
            modelName: true,
            location: true,
            year: true,
            image: true,
          },
        },
        user: {
          select: {
            username: true,
            image: true,
            phone: true,
            email: true,
          },
        },
      },
    });
    return NextResponse.json(bookings);
  } catch (error) {
    return NextResponse.error();
  } finally {
    await prisma.$disconnect();
  }
}