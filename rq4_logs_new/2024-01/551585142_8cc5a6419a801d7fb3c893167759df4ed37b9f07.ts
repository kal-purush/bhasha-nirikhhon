import prisma from "@database/prisma/instance";
import { PrismaClientKnownRequestError } from "@prisma/client/runtime/library";
import { NextResponse } from "next/server";

export const GET = async (
  _req: Request,
  { params: { username } }: { params: { username: string } },
) => {
  try {
    const responseData = {} as Record<string, number>;
    const userStats = await prisma.debugSubmissions.groupBy({
      by: ["dpid"],
      where: {
        teams: {
          teamname: username,
        },
      },
      _max: {
        score: true,
      },
      orderBy: {
        dpid: "asc",
      },
    });
    const problemsCode = await prisma.debugProblems.findMany({
      where: {
        dpid: {
          in: userStats.map(({ dpid }) => dpid),
        },
      },
      select: {
        code: true,
      },
      orderBy: {
        dpid: "asc",
      },
    });
    userStats.forEach(
      ({ _max: { score } }, idx) => (responseData[problemsCode[idx].code] = score ?? 0),
    );

    return NextResponse.json({ data: responseData }, { status: 200 });
  } catch (e) {
    if (e instanceof PrismaClientKnownRequestError) {
      console.error(e.code, e.message);
      return NextResponse.json({ error: e.message }, { status: parseInt(e.code) });
    } else {
      console.error(e);
      return NextResponse.json({ error: "Internal server error" }, { status: 500 });
    }
  }
};