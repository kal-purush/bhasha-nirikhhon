// Next.js API route support: https://nextjs.org/docs/api-routes/introduction
import type { NextApiRequest, NextApiResponse } from "next";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse<string>
) {
  const { REFRESH_TOKEN, CLIENT_ID, CLIENT_SECRET, REDIRECT_URI } = process.env;
  const response = await fetch(
    `${process.env.AUTH_URI}/token?refresh_token=${REFRESH_TOKEN}&client_id=${CLIENT_ID}&client_secret=${CLIENT_SECRET}&redirect_uri=${REDIRECT_URI}&grant_type=refresh_token`,
    { method: "POST" }
  );
  const data = await response.json();
  console.log(data)
  return res.status(200).json(data.access_token);
}