import { deleteTwoFactorTokenById, getTwoFactorTokenByEmail } from "@/services/two-factor-token";
import crypto from "crypto";
import prisma from "./db";

const generateTwoFactorToken = async (email: string) => {
  const token = crypto.randomInt(100_000, 1_000_000).toString();
  const expires = new Date(new Date().getTime() + 3600 * 1000);

  const existingToken = await getTwoFactorTokenByEmail(email);
  if (existingToken) await deleteTwoFactorTokenById(existingToken.id);

  const twoFactorToken = await prisma.twoFactorToken.create({
    data: {
      email,
      token,
      expires,
    }
  })

  return twoFactorToken;
}

export { generateTwoFactorToken }