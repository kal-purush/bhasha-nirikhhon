// src/app/api/analytics/route.ts

import { NextResponse } from "next/server";
import { prisma } from "../../../lib/prisma";

export async function GET(req: Request) {
  const url = new URL(req.url);
  const address = url.searchParams.get("address")!;
  const limit = parseInt(url.searchParams.get("limit") || "5");

  const user = await prisma.user.findUnique({
    where: { address },
    include: {
      links: {
        include: {
          clicks: {
            orderBy: { createdAt: "desc" },
            take: limit,
          },
        },
      },
    },
  });
  if (!user) return NextResponse.json({ error: "Not found" }, { status: 404 });

  const allClicks = user.links.flatMap(link => link.clicks);
  const totalClicks = allClicks.length;
  const totalUrls = user.links.length;
  const avgClicks = totalUrls ? Math.round(totalClicks / totalUrls) : 0;

  // Timeline: count clicks by day
  const timeline = await prisma.$queryRaw`
    SELECT to_char("createdAt"::date, 'Mon') AS day, COUNT(*) AS count
    FROM "Click" JOIN "Link" ON "Click"."linkId" = "Link"."id"
    WHERE "Link"."ownerId" = ${user.id}
    GROUP BY day ORDER BY MIN("createdAt")
  `;

  // Traffic sources
  const traffic = await prisma.$queryRaw`
    SELECT "source" as name, COUNT(*) AS value
    FROM "Click" JOIN "Link" ON "Click"."linkId" = "Link"."id"
    WHERE "Link"."ownerId" = ${user.id}
    GROUP BY source
  `;

  // Devices
  const devices = await prisma.$queryRaw`
    SELECT "device" as device, COUNT(*) AS clicks
    FROM "Click" JOIN "Link" ON "Click"."linkId" = "Link"."id"
    WHERE "Link"."ownerId" = ${user.id}
    GROUP BY device
  `;

  // Locations (top 5)
  const locations = await prisma.$queryRaw`
    SELECT location AS name, COUNT(*) AS clicks
    FROM "Click" JOIN "Link" ON "Click"."linkId" = "Link"."id"
    WHERE "Link"."ownerId" = ${user.id}
    GROUP BY location
    ORDER BY clicks DESC LIMIT 5
  `;

  // Top links
  const topLinks = await prisma.$queryRaw`
    SELECT "shortCode" as code, COUNT(*) AS clicks, CID as url
    FROM "Click" JOIN "Link" ON "Click"."linkId" = "Link"."id"
    WHERE "Link"."ownerId" = ${user.id}
    GROUP BY code, cid ORDER BY clicks DESC LIMIT 5
  `;

  return NextResponse.json({
    totalClicks,
    totalUrls,
    averageClicks: avgClicks,
    timeline,
    traffic,
    devices,
    locations,
    topLinks,
  });
}