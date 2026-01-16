import { prisma } from "@/lib/prismadb";
import { z } from "zod";
import bcrypt from "bcryptjs";

const handleAuth = async (
  credentials: Record<"email" | "password", string> | undefined
) => {
  if (!credentials?.password || !credentials?.email) {
    throw new Error("Invalid fields: missing email or password.");
  }

  const emailSchema = z
    .string()
    .email()
    .min(3, {
      message: "Email must be at least 3 characters.",
    })
    .max(255, {
      message: "Email must be at most 255 characters.",
    });
  const passwordSchema = z
    .string()
    .min(8, { message: "Password must be at least 8 characters." })
    .max(32, { message: "Password must be at most 32 characters." });

  try {
    emailSchema.parse(credentials.email);
    passwordSchema.parse(credentials.password);
  } catch (error: unknown) {
    if (error instanceof z.ZodError) {
      throw new Error("Invalid fields: " + error.message);
    }
  }

  const user = await prisma.user.findUnique({
    where: {
      email: credentials.email,
    },
  });

  if (!user) {
    throw new Error("User not found.");
  }

  const isPasswordMatch = await bcrypt.compare(
    credentials.password,
    user.password
  );

  if (!isPasswordMatch) throw new Error("Incorret password, try again.");

  return user;
};

export default handleAuth;