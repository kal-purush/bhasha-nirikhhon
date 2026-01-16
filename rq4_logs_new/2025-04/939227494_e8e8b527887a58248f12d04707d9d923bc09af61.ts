"use server";

import { signIn, signOut } from "@/auth";
import { DEFAULT_REDIRECTED, signinSchema, signupSchema } from "@/constants";
import { prisma } from "@/lib";
import bcrypt from "bcryptjs";
import { safeAction } from "../next-safe-action";

export const signUpAction = safeAction
  .schema(signupSchema)
  .action(async ({ parsedInput: data }) => {
    const { email, password, name, phone } = data;

    const existingUser = await prisma.user.findUnique({
      where: { email },
    });

    if (existingUser !== null) {
      throw new Error("Email already exists");
    }

    const hashedPassword = await bcrypt.hash(password, 10);

    await prisma.user.create({
      data: {
        email,
        name,
        password: hashedPassword,
        phone,
      },
    });
    return {
      success: true,
      message: "Success to register",
    };
  });

export const signInViaGoogleAction = safeAction.action(async () => {
  await signIn("google", {
    redirectTo: DEFAULT_REDIRECTED,
  });
});

export const signInAction = safeAction
  .schema(signinSchema)
  .action(async ({ parsedInput }) => {
    const { email, password } = parsedInput;

  await signIn("credentials", {
      email,
      password,
      redirect: false,
    });
  });

export const  logoutAction =safeAction.action(async ()=>{
  return await signOut();
}) 