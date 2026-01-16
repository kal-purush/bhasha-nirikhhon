import { NextRequest, NextResponse } from "next/server";
import { PrismaCli } from "../../../../prismaCli/prismaClient";
import { NextApiRequest } from "next";

export async function GET(req: NextRequest) {
  const searchQuery = req.nextUrl.searchParams.get("test");

  let NumberShow;
  NumberShow = Number(searchQuery) + 1;

  if (NumberShow > 31) {
    NumberShow = 31;
  } else if (NumberShow < 3) {
    NumberShow = 4;
  }

  const currentDate = new Date();

  const resultForLastThreeDays = [];

  for (let i = 1; i < NumberShow; i++) {
    const startOfDay = new Date(currentDate);
    startOfDay.setDate(currentDate.getDate() - i);
    startOfDay.setHours(0, 0, 0, 0);

    const endOfDay = new Date(currentDate);
    endOfDay.setDate(currentDate.getDate() - i + 1);
    endOfDay.setHours(0, 0, 0, 0);

    try {
      const dataForCurrentDay = await PrismaCli.user.findMany({
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
      return NextResponse.json(
        { message: `Fetch data for day ${i + 1} failed` },
        { status: 500 }
      );
    }
  }

  // Return the response after the loop completes
  return NextResponse.json({ resultForLastThreeDays }, { status: 200 });
}