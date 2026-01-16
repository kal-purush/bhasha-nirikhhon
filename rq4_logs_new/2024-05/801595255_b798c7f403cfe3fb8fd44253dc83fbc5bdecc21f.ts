"use server"

import { ResetPasswordSchema } from "@/schemas"
import { deletePasswordResetTokenById, getPasswordResetTokenByToken } from "@/services/reset-password-token";
import { getUserByEmail } from "@/services/user";
import { z } from "zod";
import { hash } from "bcryptjs";
import prisma from "@/lib/db";

const newPassword = async ({ token, values }: { token: string, values: z.infer<typeof ResetPasswordSchema> }) => {
  if (!token) return { error: "Missing token" }

  const result = ResetPasswordSchema.safeParse(values);
  if (!result.success) return { error: "Invalid fields" }

  const { password } = result.data;

  const existingToken = await getPasswordResetTokenByToken(token);
  if (!(existingToken && existingToken.email)) return { error: "Invalid token" };

  const hasExpired = new Date(existingToken.expires) < new Date();
  
  if (hasExpired) {
    await deletePasswordResetTokenById(existingToken.id);
    return { error: "Token has expired" };
  }

  const existingUser = await getUserByEmail(existingToken.email);
  if (!existingUser) return { error: "Email does not exist" };

  const passwordHash = await hash(password, 10);

  await prisma.user.update({
    where: {
      id: existingUser.id,
    },
    data: {
      password: passwordHash,
    }
  })

  await deletePasswordResetTokenById(existingToken.id);

  return { success: "Password updated" }
}

export { newPassword }