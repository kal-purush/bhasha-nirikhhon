import { FastifyPluginCallback } from 'fastify';
import fp from 'fastify-plugin';
import { pipeline } from 'stream';
import { promisify } from 'util';
import { listUploadQuery, ListUploadQuery, postUploadQuery, PostUploadQuery, UserUpload } from '../schemas';

const pump = promisify(pipeline);

const tags = ['upload'];

const uploadControllersPlugin: FastifyPluginCallback = function (app, _, done) {
  app.post<{ Querystring: PostUploadQuery }>(
    '/upload',
    {
      authorize: true,
      schema: {
        description: 'Upload a file',
        tags,
        querystring: postUploadQuery,
      },
    },
    async req => {
      for await (const data of req.files()) {
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        await app.services.upload.upload(data, { userId: req.user!.userId, uploadType: req.query.uploadType });
      }
    },
  );

  app.get<{ Params: { uploadId: UserUpload['id'] } }>(
    '/upload/:uploadId',
    { authorize: true, schema: { description: 'Download an uploaded file', tags } },
    async (req, res) => {
      const file = await app.services.upload.download(req.params.uploadId);
      await pump(file.stream, res.raw);
    },
  );

  app.get<{ Querystring: ListUploadQuery }>(
    '/upload',
    {
      authorize: true,
      schema: { description: 'List uploads', querystring: listUploadQuery },
    },
    req => app.repositories.userUpload.list(req.query),
  );

  done();
};

export const uploadControllers = fp(uploadControllersPlugin);