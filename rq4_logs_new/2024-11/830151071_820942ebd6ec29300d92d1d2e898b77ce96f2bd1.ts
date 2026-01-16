import { dev } from "@youmeet/functions/imports";
import { BACKEND_ERRORS, BACKEND_MESSAGES } from "@youmeet/types/api/backend";
import { BackendError } from "@youmeet/utils/basics/BackendErrorClass";
import { handleActionError } from "@youmeet/utils/basics/handleActionError";
import { NextRequest } from "next/server";

export async function GET(req: NextRequest): Promise<Response> {
  const searchParams = req.nextUrl.searchParams;

  try {
    if (
      req.cookies.get("login")?.value ||
      (req.nextUrl.origin !== "https://www.youmeet.info" && !dev) ||
      dev
    ) {
      const uri =
        "https://entreprise.francetravail.fr/connexion/oauth2/access_token";
      const client_id = `${process.env.FRANCE_TRAVAIL_CLIENT_ID}`;
      const client_secret = `${process.env.FRANCE_TRAVAIL_CLIENT_SECRET}`;
      const params = new URLSearchParams({ realm: "/partenaire" });
      const grant_type = "client_credentials";
      const scope = decodeURIComponent(searchParams.get("scope") ?? "");

      const body = `grant_type=${grant_type}&client_id=${client_id}&client_secret=${client_secret}&scope=${scope}`;

      const endpoint = uri + "?" + params.toString();

      const response = await fetch(endpoint, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
        },
        body,
      });

      const data = await response.json();

      return Response.json(data);
    } else {
      throw new BackendError(
        BACKEND_ERRORS.NOT_AUTHORIZED,
        BACKEND_MESSAGES.NOT_AUTHORIZED
      );
    }
  } catch (err: any) {
    return Response.json(await handleActionError(err), {
      status: err.status,
      statusText: err.statusText,
    });
  }
}