"use server";

import { createAccountSchema } from "@/lib/zod-schemas";

export async function createAccount(_: any, formData: FormData) {
  await new Promise((r) => setTimeout(r, 4000));
  const data = {
    nickname: formData.get("nickname"),
    id: formData.get("id"),
    email: formData.get("email"),
    password: formData.get("password"),
    confirmPassword: formData.get("confirmPassword"),
  };

  const result = createAccountSchema.safeParse(data);

  if (!result.success) {
    return result.error.flatten();
  }

  // fetch
}