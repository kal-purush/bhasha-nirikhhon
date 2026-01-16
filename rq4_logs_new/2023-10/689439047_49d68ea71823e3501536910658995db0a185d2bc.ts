import { currentProfile } from "@/lib/current-profile";
import { NextResponse } from "next/server";

export async function DELETE(
  req: Request,
  { params }: { params: { channelId: string } }
) {
  try {
    const {searchParams} = new URL(req.url)
    const profile = await currentProfile()

    const serverId = searchParams.get('serverId')

    if(!profile) {
      return new NextResponse("Unauthorized", {status: 401})
    }

    if(!serverId) {
      return new NextResponse("Server ID missing", {status: 400})
    }
  } catch (error) {
    console.log("[CHANNEL_ID_DELETE]", error);
    return new NextResponse("Internal Error", { status: 500 });
  }
}