import { PrismaClient } from '@prisma/client';
import { NextApiRequest, NextApiResponse } from 'next';
import { getSession } from 'next-auth/react';
import { storageBucket } from 'utils/firebaseUtils';
import { v4 as uuid } from 'uuid';
import { SessionWithUserId } from './auth/[...nextauth]';

const prisma = new PrismaClient();

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  const imageBuffer = Buffer.from(
    req.body.replace('data:image/png;base64,', ''),
    'base64'
  );

  if (imageBuffer.byteLength > 131072) {
    res.status(400).json({
      error: 'Image size must be less than 128 kb',
    });
    return;
  }

  const file = storageBucket.file(`${uuid()}.png`);
  const stream = file.createWriteStream();
  stream.write(imageBuffer);
  stream.end();

  await storageBucket.makePublic();
  const url = file.publicUrl();

  const session = (await getSession({ req })) as SessionWithUserId;
  await prisma.$connect();

  await prisma.user.update({
    where: {
      id: session.user.id,
    },
    data: {
      pfpUrl: url,
    },
  });

  await prisma.$disconnect();

  // TODO: DELETE USER PAST PROFILE PICTURES
  // TODO: CLEANUP / DELETE USER PROFILE PICTURES IF REQUEST FAILS

  res.status(200).end();
}