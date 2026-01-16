import prisma from "../../../lib/prisma";
import { NextApiRequest, NextApiResponse } from "next";

export default async function handle(
  req: NextApiRequest,
  res: NextApiResponse
) {
  const postId = Number(req.query.id);

  if (req.method === "GET") {
    handleGet(postId, res);
  } else if (req.method === "DELETE") {
    handleDelete(postId, res);
  } else {
    throw new Error(
      `The HTTP ${req.method} method is not supported at this route.`
    );
  }
}

// GET /api/post/:id
async function handleGet(postId: number, res: NextApiResponse) {
  const post = await prisma.post.findUnique({
    where: { id: postId },
  });
  res.json(post);
}

// DELETE /api/post/:id
async function handleDelete(postId: number, res: NextApiResponse) {
  const post = await prisma.post.delete({
    where: { id: postId },
  });
  res.json(post);
}
