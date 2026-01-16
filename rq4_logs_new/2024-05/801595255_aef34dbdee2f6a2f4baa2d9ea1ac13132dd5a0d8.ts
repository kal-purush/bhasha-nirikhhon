"use server";

import prisma from "@/lib/db";
import { getUserByEmail } from "@/services/user";
import { deleteVerificationTokenById, getVerificationTokenByToken } from "@/services/verification-token";

const newVerification = async (token: string) => {
  const existingToken = await getVerificationTokenByToken(token);
  if (!(existingToken &&existingToken.email)) return { error: "Token doesn't exist" };

  const expierd = new Date(existingToken.expires) < new Date();
  if (expierd) return { error: "Token has expired" };

  const existingUser = await getUserByEmail(existingToken.email);
  if (!existingUser) return { error: "User not found" };

  await prisma.user.update({
    where: {
      id: existingUser.id,
    },
    data: {
      email: existingToken.email,
      emailVerified: new Date(),
    }
  })

  await deleteVerificationTokenById(existingToken.id);

  return { success: "Email verefied" }
}

export { newVerification };