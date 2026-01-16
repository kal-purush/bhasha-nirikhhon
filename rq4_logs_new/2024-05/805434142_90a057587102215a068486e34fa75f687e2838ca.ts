import { NextRequest, NextResponse } from "next/server";
import { PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

export async function GET(req: NextRequest) {
  const searchQuery = req.nextUrl.searchParams.get("posts");
  let NumberShow = Number(searchQuery) + 1;

  if (NumberShow <= 4) {
    NumberShow = 3;
  } else if (NumberShow > 30) {
    NumberShow = 30;
  }

  const currentDate = new Date();
  const resultForLastThreeDays = [];

  for (let i = 0; i < NumberShow; i++) {
    // Changed loop to start from 0
    const startOfDay = new Date(currentDate);
    startOfDay.setDate(currentDate.getDate() - i);
    startOfDay.setHours(0, 0, 0, 0);

    const endOfDay = new Date(startOfDay);
    endOfDay.setDate(startOfDay.getDate() + 1);

    try {
      const dataForCurrentDay = await prisma.posts.findMany({
        where: {
          time: {
            gte: startOfDay,
            lt: endOfDay,
          },
        },
      });

      resultForLastThreeDays.push({
        day: i,
        data: dataForCurrentDay,
      });
    } catch (err) {
      console.error(`Error fetching data for day ${i}:`, err);
      return NextResponse.json(
        { message: `Fetch data for day ${i} failed` },
        { status: 500 }
      );
    }
  }

  return NextResponse.json({ resultForLastThreeDays }, { status: 200 });
}