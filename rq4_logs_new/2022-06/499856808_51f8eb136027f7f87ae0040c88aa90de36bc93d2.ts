import type { NextRequest } from "next/server";

export default function file(req: NextRequest) {
  if (req.method !== "GET") {
    throw new Error(`HTTP ${405}. Method not allowed`);
  } else {
    if (req.nextUrl.pathname === "/file") {
      return new Response("Hello world");
    }
  }
}