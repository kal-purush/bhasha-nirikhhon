// pages/api/metadata.ts
import { NextApiRequest, NextApiResponse } from "next";
import { getMetadata } from "@/utils/metaData";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  const { url } = req.query;

  if (typeof url !== "string") {
    res.status(400).json({ error: "Invalid URL parameter" });
    return;
  }

  try {
    const metadata = await getMetadata(url);
    res.status(200).json(metadata);
  } catch (error) {
    console.error(`Error fetching metadata: ${(error as Error).message}`);
    res.status(500).json({ error: "Failed to fetch metadata" });
  }
}