import { PrismaClient } from '@prisma/client';
import { NextApiRequest, NextApiResponse } from 'next';
import { getSession } from 'next-auth/react';
import { SessionWithUserId } from './auth/[...nextauth]';
import * as Yup from 'yup';

const prisma = new PrismaClient();

const schema = Yup.object().shape({
  content: Yup.string().required('You need something to chirp about'),
});

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  // get and validate session
  const session = (await getSession({ req })) as SessionWithUserId;
  if (!session) {
    res.status(401).end();
    return;
  }

  const data = req.body;

  // validate the chirp
  if (!schema.isValid(data)) {
    res.send('Invalid Chirp');
    return;
  }

  const replyToId = data.replyToId;

  await prisma.$connect();

  const chirp = await prisma.chirp.create({
    data: {
      // replace multiple new lines, max of one empty line
      content: data.content.replace(/[\n]{3,}/g, '\n\n'),
      authorId: session.user.id,
      replyToId,
    },
    include: {
      author: {
        select: {
          likes: true,
          name: true,
          username: true,
        },
      },
    },
  });

  await prisma.$disconnect();

  res.status(200).json(chirp);
}