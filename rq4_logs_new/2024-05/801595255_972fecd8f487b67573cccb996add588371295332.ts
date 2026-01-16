"use server"

import { currentUser } from "@/lib/auth";
import prisma from "@/lib/db";
import { sendVerificationEmail } from "@/lib/mail";
import { generateVerificationToken } from "@/lib/tokens";
import { SettingsSchema } from "@/schemas"
import { getUserByEmail, getUserById } from "@/services/user";
import { z } from "zod";
import { compare, hash } from "bcryptjs"

const settings = async (values: z.infer<typeof SettingsSchema>) => {
  const result = SettingsSchema.safeParse(values);
  if (!result.success) return { error: "Invalid fields" };

  const { oldPassword, confirm, ...newValues} = result.data;

  const user = await currentUser();
  if (!(user && user.id)) return { error: "Unauthorized" }

  const existingUser = await getUserById(user.id);
  if (!existingUser) return { error: "Unauthorized" }

  if (user.isOAuth) {
    newValues.email = undefined;
    newValues.password = undefined;
    newValues.isTwoFactorEnabled = undefined;
  }

  if (values.email && values.email !== user.email) {
    const validEmail = await getUserByEmail(values.email);
    if (!!validEmail && validEmail.id !== user.id) return { error: "Email already in use" }

    const verificationToken = await generateVerificationToken(values.email);
    await sendVerificationEmail({ email: verificationToken.email, token: verificationToken.token });

    return { success: "Verification email sent" }
  }

  if (!!oldPassword && !!existingUser.password && newValues.password) {
    const compareHash = await compare(oldPassword, existingUser.password);
    if (!compareHash) return { error: "Incorrect password" }

    const passwordHash = await hash(newValues.password, 10);
    newValues.password = passwordHash
  }

  await prisma.user.update({
    where: {
      id: existingUser.id
    },
    data: {
      ...newValues
    }
  })

  return { success: "The user has been updated" }
}

export { settings }