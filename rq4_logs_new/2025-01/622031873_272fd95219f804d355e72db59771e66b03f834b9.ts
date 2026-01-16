import { NextResponse } from "next/server";
import type { Session } from "next-auth";

export function checkUserAuthenticated(session: Session | null) {
  if (!session?.user?.id) {
    console.error("User not authenticated");
    return NextResponse.json({ error: "Not authenticated" }, { status: 401 });
  }
  return null;
}

export function checkUserRole(session: Session | null, allowedRoles: string[]) {
  if (!session || !allowedRoles.includes(session?.user?.role)) {
    console.error("User not authorized");
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }
  return null;
}