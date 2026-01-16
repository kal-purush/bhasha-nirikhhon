import { NextApiRequest, NextApiResponse } from 'next';
import { getSession } from 'next-auth/react';
import { SessionWithUserId } from './auth/[...nextauth]';
import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  const offset = req.query.offset ? parseInt(req.query.offset as string) : 0;
  const chirpId = req.query.chirpId ? (req.query.chirpId as string) : undefined;

  if (!chirpId) {
    res.status(400).end();
    return;
  }

  const session = (await getSession({ req })) as SessionWithUserId;

  await prisma.$connect();

  const chirp = await prisma.chirp.findFirst({
    where: {
      id: chirpId,
    },
    select: {
      replies: {
        orderBy: {
          createdAt: 'desc',
        },
        take: 10,
        skip: offset,
        select: {
          content: true,
          createdAt: true,
          id: true,
          author: {
            select: {
              name: true,
              username: true,
              id: true,
              pfpUrl: true,
            },
          },
          authorId: true,
          numLikes: true,

          // query likes which come from the user on the post,
          // if it does not exist, it means the user has not liked it
          // only works if the user is logged in
          likes: !!session && {
            where: {
              userId: session.user.id,
            },
          },
        },
      },
    },
  });

  chirp.replies.map((v) => {
    const liked = v.likes.length !== 0;

    delete v.likes;

    if (!!session)
      return {
        ...v,
        liked,
      };

    // dont add a `liked` property if the user is not signed in
    return v;
  });
  res.json(chirp.replies);

  await prisma.$disconnect();
}