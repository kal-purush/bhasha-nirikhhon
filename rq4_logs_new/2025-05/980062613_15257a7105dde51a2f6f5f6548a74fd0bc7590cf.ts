import { NextResponse } from "next/server";
import { prisma } from "../../../lib/prisma";

export async function POST(request: Request) {
  try {
    const { shortCode, cid, ownerAddress } = await request.json();

    // Validate inputs
    if (!shortCode || !cid || !ownerAddress) {
      return NextResponse.json({ error: "Missing required fields" }, { status: 400 });
    }

    const shortCodeRegex = /^[a-zA-Z0-9]{6,10}$/;
    if (!shortCodeRegex.test(shortCode)) {
      return NextResponse.json({ error: "Invalid shortCode format" }, { status: 400 });
    }

    // Support CIDv0 (Qm...) and CIDv1 (b...)
    const cidRegex = /^(Qm[1-9A-Za-z]{44}|[bcdf][a-zA-Z0-9+\/]{58,})$/;
    if (!cidRegex.test(cid)) {
      return NextResponse.json({ error: "Invalid CID format" }, { status: 400 });
    }

    if (!/^0x[a-fA-F0-9]{40}$/.test(ownerAddress)) {
      return NextResponse.json({ error: "Invalid ownerAddress format" }, { status: 400 });
    }

    const user = await prisma.user.findUnique({ where: { address: ownerAddress } });
    if (!user) {
      return NextResponse.json({ error: "User not found" }, { status: 404 });
    }

    const link = await prisma.link.create({
      data: {
        shortCode,
        cid,
        ownerId: user.id,
      },
    });

    return NextResponse.json({ success: true, link });
  } catch (error: any) {
    console.error("Link Creation Error:", error);
    return NextResponse.json({ error: "Internal server error", details: error.message }, { status: 500 });
  }
}