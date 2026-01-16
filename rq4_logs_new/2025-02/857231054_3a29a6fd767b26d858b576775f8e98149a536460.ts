import bcrypt from "bcryptjs";
import { NextResponse } from "next/server";
import { prisma } from "@/lib/prisma";
import { verifyToken } from "@/utils/authMiddleware";

export async function PATCH(
  req: Request,
  { params }: { params: { id: string } }
) {
  try {
    const authHeader = req.headers.get("authorization");

    if (!authHeader || !authHeader.startsWith("Bearer ")) {
      return NextResponse.json({ message: "Unauthorized" }, { status: 401 });
    }

    const token = authHeader.split(" ")[1];
    const user = await verifyToken(token);

    if (!user) {
      return NextResponse.json({ message: "Invalid token" }, { status: 401 });
    }

    const userId = parseInt(params.id);
    if (user.id !== userId) {
      return NextResponse.json({ message: "Forbidden" }, { status: 403 });
    }

    const { username, oldPassword, newPassword } = await req.json();

    const existingUser = await prisma.user.findUnique({
      where: { id: userId },
    });

    if (!existingUser) {
      return NextResponse.json({ message: "User not found" }, { status: 404 });
    }

    const updateData: { username?: string; password?: string } = {};

    if (username) {
      updateData.username = username;
    }

    if (newPassword && oldPassword) {
      const isPasswordValid = await bcrypt.compare(
        oldPassword,
        existingUser.password
      );
      if (!isPasswordValid) {
        return NextResponse.json(
          { message: "Incorrect old password" },
          { status: 400 }
        );
      }

      const hashedPassword = await bcrypt.hash(newPassword, 10);
      updateData.password = hashedPassword;
    }

    const updatedUser = await prisma.user.update({
      where: { id: userId },
      data: updateData,
      select: { id: true, username: true, createdAt: true },
    });

    return NextResponse.json(
      { success: true, data: updatedUser },
      { status: 200 }
    );
  } catch (error) {
    return NextResponse.json(
      { message: "Internal Server Error" },
      { status: 500 }
    );
  }
}

export async function PUT(
  req: Request,
  { params }: { params: { id: string } }
) {
  try {
    const authHeader = req.headers.get("authorization");

    if (!authHeader || !authHeader.startsWith("Bearer ")) {
      return NextResponse.json({ message: "Unauthorized" }, { status: 401 });
    }

    const token = authHeader.split(" ")[1];
    const user = await verifyToken(token);

    if (!user) {
      return NextResponse.json({ message: "Invalid token" }, { status: 401 });
    }

    const userId = parseInt(params.id);
    if (user.id !== userId) {
      return NextResponse.json({ message: "Forbidden" }, { status: 403 });
    }

    const { username, oldPassword, newPassword } = await req.json();

    const existingUser = await prisma.user.findUnique({
      where: { id: userId },
    });

    if (!existingUser) {
      return NextResponse.json({ message: "User not found" }, { status: 404 });
    }

    const updateData: { username?: string; password?: string } = {};

    if (username) {
      updateData.username = username;
    }

    if (newPassword && oldPassword) {
      const isPasswordValid = await bcrypt.compare(
        oldPassword,
        existingUser.password
      );
      if (!isPasswordValid) {
        return NextResponse.json(
          { message: "Incorrect old password" },
          { status: 400 }
        );
      }

      const hashedPassword = await bcrypt.hash(newPassword, 10);
      updateData.password = hashedPassword;
    }

    const updatedUser = await prisma.user.update({
      where: { id: userId },
      data: updateData,
      select: { id: true, username: true, createdAt: true },
    });

    return NextResponse.json(
      { success: true, data: updatedUser },
      { status: 200 }
    );
  } catch (error) {
    return NextResponse.json(
      { message: "Internal Server Error" },
      { status: 500 }
    );
  }
}