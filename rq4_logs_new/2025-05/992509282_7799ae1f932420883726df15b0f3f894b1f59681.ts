import { NextRequest, NextResponse } from "next/server";
import { getValidStravaAccessToken } from "@/strava";
import { auth } from "@/auth";

export async function GET(req: NextRequest, { params }: { params: { id: string } }) {
  try {
    const session = await auth();
    if (!session || !session.user?.id) {
      return NextResponse.json({ error: "Not authenticated" }, { status: 401 });
    }
    const userId = session.user.id;
    const accessToken = await getValidStravaAccessToken(userId);
    const activityId = params.id;
    const url = `https://www.strava.com/api/v3/activities/${activityId}/streams?keys=latlng&key_by_type=true`;
    const stravaRes = await fetch(url, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
      },
    });
    if (!stravaRes.ok) {
      const error = await stravaRes.json();
      return NextResponse.json({ error: error.message || "Failed to fetch activity streams from Strava" }, { status: stravaRes.status });
    }
    const data = await stravaRes.json();
    return NextResponse.json(data);
  } catch (e: any) {
    return NextResponse.json({ error: e.message || "Unknown error" }, { status: 500 });
  }
} 