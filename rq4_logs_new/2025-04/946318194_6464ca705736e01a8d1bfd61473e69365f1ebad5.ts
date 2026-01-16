import { NextRequest } from "next/server";

export async function POST(req: NextRequest) {
  const secret = req.nextUrl.searchParams.get("secret");
  const validSecret = process.env.REVALIDATE_SECRET;

  if (secret !== validSecret) {
    return new Response("Invalid token", { status: 401 });
  }

  const body = await req.json();
  const slug = body.slug;

  try {
    // If the slug is empty or "home", revalidate the root
    const path = !slug || slug === "home" ? "/" : `/${slug}`;
    await fetch(`${process.env.NEXT_PUBLIC_SITE_URL}${path}`, {
      method: "PURGE",
    });

    return new Response("Revalidated", { status: 200 });
  } catch (err) {
    console.error("Revalidation failed:", err);
    return new Response("Failed to revalidate", { status: 500 });
  }
}