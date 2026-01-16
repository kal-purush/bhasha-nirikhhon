import { NextResponse } from "next/server";
import prisma from "@/lib/prisma";

// This is a test route to verify our User model
export async function GET() {
  try {
    // Create a test user
    const testUser = await prisma.user.create({
      data: {
        clerk_id: "test_" + Date.now().toString(),
        email: "test@example.com",
        name: "Test User",
      },
    });

    // Fetch all users
    const allUsers = await prisma.user.findMany();

    return NextResponse.json({
      message: "User model is working correctly!",
      testUser,
      allUsers,
      count: allUsers.length,
    });
  } catch (error) {
    console.error("Error testing user model:", error);
    return NextResponse.json({ error: "Failed to test user model" }, { status: 500 });
  }
}