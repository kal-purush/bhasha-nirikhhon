import { z } from "zod";

const passwordSchema = z
  .string()
  .min(8, "Password should be at least 8 characters")
  .max(100, "Password should be at most 100 characters")
  .refine((value) => /[A-Z]/.test(value), {
    message: "Password should contain at least one uppercase letter",
  })
  .refine((value) => /[a-z]/.test(value), {
    message: "Password should contain at least one lowercase letter",
  })
  .refine((value) => /[0-9]/.test(value), {
    message: "Password should contain at least one number",
  })
  .refine((value) => /[@$!%*#?&]/.test(value), {
    message:
      "Password should contain at least one special character (@, $, !, %, *, #, ?, &)",
  });

const userSchema = z.object({
  userName: z
    .string()
    .min(4, "Username should be at least 4 characters")
    .max(20, "Username should be at most 20 characters"),
  password: passwordSchema,
});

const signUpSchema = z
  .object({
    ...userSchema.shape,
    email: z.string().email(),
    confirmPassword: z.string(),
  })
  .refine((data) => data.password === data.confirmPassword, {
    message: "Passwords do not match",
    path: ["confirmPassword"],
  });

export type signUpType = z.infer<typeof signUpSchema>;

const defaultValues: signUpType = {
  userName: "",
  email: "",
  password: "",
  confirmPassword: "",
};

export const UserSchema = {
  signUpSchema,
  defaultValues,
};