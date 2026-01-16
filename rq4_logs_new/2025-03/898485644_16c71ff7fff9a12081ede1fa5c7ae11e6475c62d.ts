import { NextApiRequest, NextApiResponse } from "next";
import { prisma } from "@/lib/prisma";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  const { query } = req.query;

  if (!query || typeof query !== "string") {
    return res
      .status(400)
      .json({ message: "検索キーワードを提供してください" });
  }

  try {
    const results = await prisma.post.findMany({
      where: {
        title: {
          contains: query,
          mode: "insensitive",
        },
      },
    });
    res.status(200).json(results);
  } catch (error) {
    res.status(500).json({ message: "検索中にエラーが発生しました", error });
  }
}