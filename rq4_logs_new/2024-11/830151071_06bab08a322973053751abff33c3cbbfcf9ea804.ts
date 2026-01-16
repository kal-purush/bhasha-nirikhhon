import { NextRequest } from "next/server";
import verifyTokenServer from "@youmeet/utils/basics/verifyTokenServer";
import { cookies } from "next/headers";
import { redirect } from "next/navigation";

export async function GET(req: NextRequest) {
  const verified = await verifyTokenServer();
  if (verified) {
    (await cookies()).delete({
      name: process.env.APP === "candidate" ? "login" : "loginPro",
      path: "/",
      domain: `${process.env.API_DOMAIN}`,
    });
    return redirect("/");
  }
  return Response.json({ data: "Not authenticated" }, { status: 400 });
}