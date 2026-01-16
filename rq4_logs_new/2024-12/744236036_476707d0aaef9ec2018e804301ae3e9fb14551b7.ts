import { NextResponse } from "next/server";
import { prisma } from "@/db/prisma";

export async function GET() {
  try {
    const users = await prisma.user.findMany({
      select: {
        username: true,
        email: true,
        image: true,
        phone: true,
        createdAt: true,
        role: true,
        _count: {
          select: {
            bookings: true,
          },
        },
      },
    });
    return NextResponse.json(users);
  } catch (error) {
    return NextResponse.error();
  } finally {
    await prisma.$disconnect();
  }
}