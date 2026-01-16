"use server";
import { prisma } from "@/db/prisma";
import * as jose from "jose";
import { cookies } from "next/headers";
const JWT_SECRET = new TextEncoder().encode(process.env.JWT_SECRET);

export async function authenticateGoogleLogin(email: string) {
  const cookieStore = await cookies();
  try {
    const user = await prisma.user.findUnique({
      where: { email: email },
    });
    if (user) {
      const token = await new jose.SignJWT({
        userId: user.id,
        email: user.email,
        role: user.role,
      })
        .setProtectedHeader({ alg: "HS256" })
        .setExpirationTime("8h")
        .sign(JWT_SECRET);
      cookieStore.set("token", token, {
        httpOnly: true,
        maxAge: 8 * 60 * 60,
        sameSite: "strict",
      });
      return { success: true, message: "User updated successfully" };
    } else {
      return { success: false, error: "User not found" };
    }
  } catch (error) {
    console.error(error);
    return { success: false, error: "Something went wrong" };
  }
}