import { PrismaClient, Prisma } from "@prisma/client";
import { NextResponse } from "next/server";

const prisma = new PrismaClient();

interface UserRequest {
  name: string;
  email: string;
}

export async function POST(req: Request) {
  try {
    const body: UserRequest = await req.json();

    if (!body.name || !body.email) {
      return NextResponse.json({ error: "Both name and email are required." }, { status: 400 });
    }

    const newUser = await prisma.user.create({
      data: { name: body.name, email: body.email },
    });

    return NextResponse.json({ success: true, data: newUser }, { status: 200 });
  } catch (error: unknown) {
    console.error("Error creating user:", error);

    if (error instanceof Prisma.PrismaClientKnownRequestError && error.code === "P2002") {
      return NextResponse.json({ error: "User with this email already exists." }, { status: 400 });
    }

    return NextResponse.json({ error: "Internal server error" }, { status: 500 });
  }
}