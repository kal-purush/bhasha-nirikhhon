"use server";
import { prisma } from "@/db/prisma";
import { sendResetPasswordEmail } from "@/emails";
export async function handleResetPassword(email: string) {
  const e = email.toLowerCase();
  try {
    const user = await prisma.user.findUnique({
      where: { email: e },
    });
    if (!user) {
      throw new Error("Oops! We couldn't find your account");
    }
    const result = await sendResetPasswordEmail(
      user.email,
      user.id,
      user.username
    );
    return result.message;
  } catch (error: any) {
    console.error(error);
    // Preserve specific error messages
    throw new Error(error.message || "Something went wrong");
  }
}