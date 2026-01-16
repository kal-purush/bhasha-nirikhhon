import { logRequestError } from "../error.logger";

export async function getArbeidsforhold(onBehalfOfToken: string): Promise<Response> {
  const url = `${process.env.API_BASE_URL}/arbeidsforhold`;
  const response = await fetch(url, {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
      Authorization: `Bearer ${onBehalfOfToken}`,
    },
  });

  if (!response.ok) {
    logRequestError(response.statusText, undefined, "Feil i uthenting av arbeidsforhold");

    return new Response("Feil i uthenting av arbeidsforhold", {
      status: response.status,
    });
  }

  return response;
}