import { randomUUID } from 'crypto';
import { FastifyInstance } from 'fastify';
import { createWriteStream } from 'fs';
import { extname, resolve } from 'path';
import { pipeline } from 'stream';
import { promisify } from 'util';

const pump = promisify(pipeline);

export async function uploadRoutes(app: FastifyInstance) {
  app.post('/upload', async (request, reply) => {
    const upload = await request.file({
      limits: {
        fileSize: 5_242_880, // 5MB
      },
    });

    if (!upload) {
      return reply.status(400).send();
    }

    const mimeTypeRegex = /^(image|video)\[a-zA-Z]+/;
    const isValidFileFormat = mimeTypeRegex.test(upload.mimetype);

    if (!isValidFileFormat) {
      return reply.status(400).send();
    }

    const fileId = randomUUID();
    const extension = extname(upload.filename);

    const filename = fileId.concat(extension);

    const writeStrem = createWriteStream(
      resolve(__dirname, '../../uploads', filename)
    );

    await pump(upload.file, writeStrem);

    const fullUrl = request.protocol.concat('://').concat(request.hostname);
    const fileUrl = new URL(`/uploads/${filename}`, fullUrl).toString();

    return { fileUrl };
  });
}