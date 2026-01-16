"use server";

import { cookies } from "next/headers";

interface ICurrentUser {
  id: number;
  email: string;
}

export async function getCurrentUser(): Promise<ICurrentUser | undefined> {
  try {
    const cookieStore = await cookies();
    const jwtCookie = cookieStore.get("jwt");
    const res = await fetch("http://auth-srv:3000/api/users/currentuser", {
      method: "POST",
      credentials: "include",
      headers: {
        "Content-Type": "application/json",
      },
    });

    if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
    return res.json();
  } catch (error) {
    console.error("Failed to fetch current user:", error);
  }
}