"use server";

import { PASSWORD_RESET } from "@/functions/api";
import { State } from "@/interfaces/form";

export default async function passwordReset(
  state: State,
  formData: FormData
): Promise<State> {
  const password = formData.get("password") as string | null;
  const login = formData.get("login") as string | null;
  const key = formData.get("key") as string | null;
  const URL = PASSWORD_RESET();

  try {
    if (!login || !key || !password) throw new Error("Fill all fields");

    if (password.length < 6)
      throw new Error("Password must be at least 6 characters long");

    const response = await fetch(URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ login, key, password }),
    });

    if (!response.ok) throw new Error("Not authorized to reset password");

    return { ok: true, error: "", data: "/login" };
  } catch (error: unknown) {
    return { ok: false, error: (error as Error).message, data: null };
  }
}