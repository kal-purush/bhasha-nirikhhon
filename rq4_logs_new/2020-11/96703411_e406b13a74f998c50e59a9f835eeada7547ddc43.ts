/*import { Directory, File } from '../models';
import { app } from '../app';

import supertest from 'supertest';
import jwt from 'jsonwebtoken';
import fs from 'fs';

import { config } from '../config';

const allAccessToken = jwt.sign({ scope: 'filebank:read filebank:write filebank:delete' }, 'v3rys3cr3tK3y');

export const createDirectory = (obj: any) => new Directory(obj).save();
export const createFile = (obj: any) => new File(obj).save();

type BaseCallback = (...filePieces: string[]) => void;

const getRoutes = (fileVerifyCb: BaseCallback) => {
  describe('GET route', () => {
    it('can list items in root directory', async () => {
      await Promise.all([
        createDirectory({ name: 'test', refId: 'test' }),
        createDirectory({ name: 'test2', refId: 'test2' }),
        createDirectory({ name: 'test3', refId: 'test3' }),
        createFile({ name: 'test.txt', refId: 'test.txt', mimetype: 'application/octet-stream' }),
        createFile({ name: 'test2.txt', refId: 'test2.txt', mimetype: 'application/octet-stream' }),
        createFile({ name: 'test3.txt', refId: 'test3.txt', mimetype: 'application/octet-stream' }),
      ]);
      return supertest(app)
        .get('/')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .expect('Content-Type', /json/)
        .expect(200)
        .then((res) => {
          expect(res.body.length).toEqual(6);
        });
    });

    it('can list items in subdirectory', async () => {
      const directory = await createDirectory({ name: 'subdir', refId: 'subdir' });
      await Promise.all([
        createDirectory({ name: 'test', refId: 'test', parent: directory._id }),
        createDirectory({ name: 'test2', refId: 'test2', parent: directory._id }),
        createFile({ name: 'test.txt', refId: 'test.txt', mimetype: 'application/octet-stream', directory: directory._id }),
        createFile({ name: 'test2.txt', refId: 'test2.txt', mimetype: 'application/octet-stream', directory: directory._id }),
      ]);
      return supertest(app)
        .get('/subdir')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .expect('Content-Type', /json/)
        .expect(200)
        .then((res) => {
          expect(res.body.length).toEqual(4);
        });
    });

    it('cannot list items in non-existing directory', async () => {
      await supertest(app)
        .get('/nonexisting')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .expect(404);
      expect(await fileVerifyCb('nonexisting')).toEqual(false);
    });

    it('can get file content', async () => {
      await supertest(app)
        .post('/')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .field('type', 'file')
        .attach('file', `${__dirname}/resources/64-64.jpg`)
        .expect('Content-Type', /json/)
        .expect(200);
      expect(await fileVerifyCb('64-64.jpg')).toEqual(true);
      await supertest(app)
        .get('/64-64.jpg')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .expect(200)
        .expect('Content-Type', 'image/jpeg')
        .then((res) => {
          expect(res.body instanceof Buffer).toBeTruthy();
          const file = fs.readFileSync(`${__dirname}/resources/64-64.jpg`);
          expect(res.body.length).toEqual(file.length);
        });
    });
  });
};

const postRoutes = (fileVerifyCb: BaseCallback) => {
  describe('POST route', () => {
    it('can create directory on fs root', async () => {
      const dir = await supertest(app)
        .post('/')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .send({ name: 'test-1', type: 'directory' })
        .expect('Content-Type', /json/)
        .expect(200)
        .then((res) => {
          expect(res.body.refId.startsWith('test-1')).toBeTruthy();
          expect(res.body.type).toEqual('directory');
        });
      expect(await fileVerifyCb('test-1')).toEqual(true);
      return dir;
    });

    it('can create directory on subdir', async () => {
      const subdir = await supertest(app)
        .post('/')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .send({ name: 'test-2', type: 'directory' })
        .expect('Content-Type', /json/)
        .expect(200)
        .then((res) => {
          expect(res.body.refId.startsWith('test-2')).toBe(true);
          expect(res.body.type).toEqual('directory');
          return res.body;
        });
      const d = await supertest(app)
        .post('/test-2')
        .send({ name: 'test-3', type: 'directory' })
        .set('Authorization', `Bearer ${allAccessToken}`)
        .expect('Content-Type', /json/)
        .expect(200)
        .then((res) => {
          expect(res.body.refId.startsWith('test-2/test-3')).toBe(true);
          expect(res.body.type).toEqual('directory');
          expect(res.body.parent).toEqual(subdir._id);
        });
      expect(await fileVerifyCb('test-2', 'test-3')).toEqual(true);
      return d;
    });

    it('cannot create directory with invalid metadata', async () => {
      await supertest(app)
        .post('/')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .send({
          name: 'faildir',
          type: 'directory',
          metadata: {
            deepMeta: { thing: 'this should be number', another: 2, third: false },
            metaField: 'nottrue',
          },
          schema: 'Test',
        })
        .expect('Content-Type', /json/)
        .expect(400);
      expect(await fileVerifyCb('faildir')).toEqual(false);
    });

    it('can create file on root dir', async () => {
      const metadata = {
        deepMeta: { thing: 1, another: 2, third: 'third' },
        metaField: true,
      };
      await supertest(app)
        .post('/')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .field('metadata', JSON.stringify(metadata))
        .field('type', 'file')
        .field('schema', 'Test')
        .attach('file', `${__dirname}/resources/fire.jpg`)
        .expect('Content-Type', /json/)
        .expect(200)
        .then((res) => {
          expect(res.body.mimetype).toEqual('image/jpeg');
          expect(res.body.refId.startsWith('fire.jpg')).toBe(true);
          expect(res.body.type).toEqual('file');
          expect(res.body.metadata).toEqual(metadata);
          expect(res.body.metadata.deepMeta.another).toEqual(2);
        });
      expect(await fileVerifyCb('fire.jpg')).toEqual(true);
    });

    it('can create file on sub dir', async () => {
      await supertest(app)
        .post('/')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .send({ name: 'test-4', type: 'directory' })
        .expect('Content-Type', /json/)
        .expect(200)
        .then((res) => {
          expect(res.body.refId.startsWith('test-4')).toEqual(true);
          expect(res.body.type).toEqual('directory');
        });
      const metadata = {
        deepMeta: { thing: 1, another: 2, third: 'third' },
        metaField: true,
      };
      await supertest(app)
        .post('/test-4')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .field('metadata', JSON.stringify(metadata))
        .field('type', 'file')
        .field('schema', 'Test')
        .attach('file', `${__dirname}/resources/fire.pdf`)
        .expect('Content-Type', /json/)
        .expect(200)
        .then((res) => {
          expect(res.body.mimetype).toEqual('application/pdf');
          expect(res.body.refId.startsWith('test-4/fire.pdf')).toEqual(true);
          expect(res.body.type).toEqual('file');
          expect(res.body.metadata).toEqual(metadata);
          expect(res.body.metadata.deepMeta.third).toEqual('third');
        });
      expect(await fileVerifyCb('test-4', 'fire.pdf')).toEqual(true);
    });

    it('can create file with different name', async () => {
      await supertest(app)
        .post('/')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .field('metadata', JSON.stringify({
          deepMeta: { thing: 1, another: 2, third: 'third' },
          metaField: true,
        }))
        .field('type', 'file')
        .field('name', 'another-fire.jpg')
        .field('schema', 'Test')
        .attach('file', `${__dirname}/resources/fire.jpg`)
        .expect('Content-Type', /json/)
        .expect(200)
        .then((res) => {
          expect(res.body.mimetype).toEqual('image/jpeg');
          expect(res.body.refId.startsWith('another-fire.jpg')).toEqual(true);
          expect(res.body.type).toEqual('file');
          expect(res.body.metadata.deepMeta.another).toEqual(2);
        });
      expect(await fileVerifyCb('another-fire.jpg')).toEqual(true);
    });

    it('cannot create file when schema is missing and required', async () => {
      config.schemaRequired = true;
      try {
        const promise = await supertest(app)
          .post('/')
          .set('Authorization', `Bearer ${allAccessToken}`)
          .field('metadata', JSON.stringify({}))
          .field('type', 'file')
          .attach('file', `${__dirname}/resources/fire.pdf`)
          .expect('Content-Type', /json/)
          .expect(400);
        expect(await fileVerifyCb('fire.pdf')).toEqual(false);
        return promise;
      } finally {
        config.schemaRequired = false;
      }
    });

    it('cannot create directory when schema is missing and required', async () => {
      config.schemaRequired = true;
      try {
        await supertest(app)
          .post('/')
          .set('Authorization', `Bearer ${allAccessToken}`)
          .send({
            name: 'faildir',
            type: 'directory',
            metadata: {
              deepMeta: { thing: 1, another: 2, third: 'third' },
              metaField: true,
            },
          })
          .expect('Content-Type', /json/)
          .expect(400);
      } finally {
        config.schemaRequired = false;
      }
    });
  });
};

const putMetaRoutes = (fileVerifyCb: BaseCallback) => {
  describe('PUT .meta route', () => {
    it('cannot create file with invalid metadata', async () => {
      const promise = await supertest(app)
        .post('/')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .field('metadata', JSON.stringify({
          deepMeta: { thing: 'this should be number', another: 2, third: false },
          metaField: 'nottrue',
        }))
        .field('type', 'file')
        .field('schema', 'Test')
        .attach('file', `${__dirname}/resources/fire.pdf`)
        .expect('Content-Type', /json/)
        .expect(400);
      expect(await fileVerifyCb('fire.pdf')).toEqual(false);
      return promise;
    });

    it('can update metadata for directory', async () => {
      const dir = await supertest(app)
        .post('/')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .send({
          name: 'metadir',
          type: 'directory',
          metadata: {
            deepMeta: { thing: 1, another: 2, third: 'third' },
            metaField: true,
          },
          schema: 'Test',
        })
        .expect('Content-Type', /json/)
        .expect(200)
        .then((res) => {
          expect(res.body.refId.startsWith('metadir')).toEqual(true);
          expect(res.body.type).toEqual('directory');
          return res.body;
        });
      return await supertest(app)
        .put(`/${dir._id}.meta`)
        .set('Authorization', `Bearer ${allAccessToken}`)
        .send({
          deepMeta: { thing: 2, another: 4, third: 'fourth' },
          metaField: false,
          schema: 'Test',
        })
        .expect('Content-Type', /json/)
        .expect(200)
        .then((res) => {
          expect(res.body.metadata.metaField).toEqual(false);
          expect(res.body.metadata.deepMeta.thing).toEqual(2);
          expect(res.body.metadata.deepMeta.another).toEqual(4);
          expect(res.body.metadata.deepMeta.third).toEqual('fourth');
        });
    });

    it('cannot update invalid metadata for directory', async () => {
      const dir = await supertest(app)
        .post('/')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .send({
          name: 'metadir-2',
          type: 'directory',
          metadata: {
            deepMeta: { thing: 1, another: 2, third: 'third' },
            metaField: true,
          },
          schema: 'Test',
        })
        .expect('Content-Type', /json/)
        .expect(200)
        .then((res) => {
          expect(res.body.refId.startsWith('metadir-2')).toEqual(true);
          expect(res.body.type).toEqual('directory');
          return res.body;
        });
      await supertest(app)
        .put(`/${dir._id}.meta`)
        .set('Authorization', `Bearer ${allAccessToken}`)
        .send({
          deepMeta: { thing: 'should be number', another: 4, third: false },
          metaField: 'should be boolean',
          schema: 'Test',
        })
        .expect('Content-Type', /json/)
        .expect(400);
    });

    it('can update file metadata', async () => {
      const file = await supertest(app)
        .post('/')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .field('metadata', JSON.stringify({
          deepMeta: { thing: 1, another: 2, third: 'third' },
          metaField: true,
        }))
        .field('type', 'file')
        .field('name', 'meta.pdf')
        .field('schema', 'Test')
        .attach('file', `${__dirname}/resources/fire.pdf`)
        .expect('Content-Type', /json/)
        .expect(200)
        .then(res => res.body);
      await supertest(app)
        .put(`/${file._id}.meta`)
        .set('Authorization', `Bearer ${allAccessToken}`)
        .send({
          deepMeta: { thing: 2, another: 4, third: 'fourth' },
          metaField: false,
          schema: 'Test',
        })
        .expect('Content-Type', /json/)
        .expect(200)
        .then((res) => {
          expect(res.body.metadata.metaField).toEqual(false);
          expect(res.body.metadata.deepMeta.thing).toEqual(2);
          expect(res.body.metadata.deepMeta.another).toEqual(4);
          expect(res.body.metadata.deepMeta.third).toEqual('fourth');
        });
    });

    it('cannot update file with invalid metadata', async () => {
      const file = await supertest(app)
        .post('/')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .field('metadata', JSON.stringify({
          deepMeta: { thing: 1, another: 2, third: 'third' },
          metaField: true,
        }))
        .field('type', 'file')
        .field('name', 'meta.pdf')
        .field('schema', 'Test')
        .attach('file', `${__dirname}/resources/fire.pdf`)
        .expect('Content-Type', /json/)
        .expect(200)
        .then(res => res.body);
      await supertest(app)
        .put(`/${file._id}.meta`)
        .set('Authorization', `Bearer ${allAccessToken}`)
        .send({
          deepMeta: { thing: 'should be number', another: 4, third: 'fourth' },
          metaField: 'should be boolean',
          schema: 'Test',
        })
        .expect('Content-Type', /json/)
        .expect(400);
    });

    it('cannot update metadata non-existing item', async () => {
      await supertest(app)
        .put('/ffffffffffffffffffffffff.meta')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .send({
          deepMeta: { thing: 2, another: 4, third: 'fourth' },
          metaField: false,
          schema: 'Test',
        })
        .expect(404);
    });
  });
};

const deleteRoutes = (fileVerifyCb: BaseCallback) => {
  describe('DELETE route', () => {
    it('can delete directory on fs root', async () => {
      await supertest(app)
        .post('/')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .send({ name: 'deletethis', type: 'directory' })
        .expect('Content-Type', /json/)
        .expect(200)
        .then((res) => {
          expect(res.body.refId.startsWith('deletethis')).toEqual(true);
          expect(res.body.type).toEqual('directory');
        });
      await supertest(app)
        .delete('/deletethis')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .expect(204);
      expect(await fileVerifyCb('deletethis')).toEqual(false);
    });

    it('cannot delete non-existing directory', async () => {
      await supertest(app)
        .delete('/nonexisting')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .expect(404);
      expect(await fileVerifyCb('nonexisting')).toEqual(false);
    });

    it('can delete file on fs root', async () => {
      await supertest(app)
        .post('/')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .field('type', 'file')
        .attach('file', `${__dirname}/resources/64-64.jpg`)
        .expect('Content-Type', /json/)
        .expect(200);
      expect(await fileVerifyCb('64-64.jpg')).toEqual(true);
      await supertest(app)
        .delete('/64-64.jpg')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .expect(204);
      expect(await fileVerifyCb('64-64.jpg')).toEqual(false);
    });

    it('cannot delete non-existing file', async () => {
      await supertest(app)
        .delete('/nonexisting.jpg')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .expect(404);
      expect(await fileVerifyCb('nonexisting.jpg')).toEqual(false);
    });
  });
};

const putRoutes = (fileVerifyCb: BaseCallback) => {
  describe('PUT route', () => {
    it('can move file', async () => {
      await supertest(app)
        .post('/')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .field('type', 'file')
        .field('name', 'moveme.jpg')
        .attach('file', `${__dirname}/resources/64-64.jpg`)
        .expect('Content-Type', /json/)
        .expect(200);
      expect(await fileVerifyCb('moveme.jpg')).toEqual(true);
      await supertest(app)
        .post('/')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .send({ name: 'movehere', type: 'directory' })
        .expect('Content-Type', /json/)
        .expect(200)
        .then((res) => {
          expect(res.body.refId.startsWith('movehere')).toEqual(true);
          expect(res.body.type).toEqual('directory');
        });
      await supertest(app)
        .put('/moveme.jpg')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .send({ target: '/movehere' })
        .expect('Content-Type', /json/)
        .expect(200)
        .then((res) => {
          expect(res.body.refId.startsWith('movehere/moveme.jpg')).toEqual(true);
          expect(res.body.type).toEqual('file');
        });
      expect(await fileVerifyCb('movehere', 'moveme.jpg')).toEqual(true);
    });

    it('cannot move file to nonexisting directory', async () => {
      await supertest(app)
        .post('/')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .field('type', 'file')
        .field('name', 'moveme2.jpg')
        .attach('file', `${__dirname}/resources/64-64.jpg`)
        .expect('Content-Type', /json/)
        .expect(200);
      expect(await fileVerifyCb('moveme2.jpg')).toEqual(true);
      await supertest(app)
        .put('/moveme2.jpg')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .send({ target: '/movehere' })
        .expect(404);
      expect(await fileVerifyCb('movehere', 'moveme2.jpg')).toEqual(false);
    });

    it('can move directory', async () => {
      await supertest(app)
        .post('/')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .send({ name: 'movethisdir', type: 'directory' })
        .expect('Content-Type', /json/)
        .expect(200)
        .then((res) => {
          expect(res.body.refId.startsWith('movethisdir')).toEqual(true);
          expect(res.body.type).toEqual('directory');
        });
      await supertest(app)
        .post('/')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .send({ name: 'moveheredir', type: 'directory' })
        .expect('Content-Type', /json/)
        .expect(200)
        .then((res) => {
          expect(res.body.refId.startsWith('moveheredir')).toEqual(true);
          expect(res.body.type).toEqual('directory');
        });
      await supertest(app)
        .post('/movethisdir')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .field('type', 'file')
        .field('name', 'moveme.jpg')
        .attach('file', `${__dirname}/resources/64-64.jpg`)
        .expect('Content-Type', /json/)
        .expect(200);
      expect(await fileVerifyCb('movethisdir', 'moveme.jpg')).toEqual(true);
      await supertest(app)
        .put('/movethisdir')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .send({ target: '/moveheredir' })
        .expect('Content-Type', /json/)
        .expect(200)
        .then((res) => {
          expect(res.body.refId.startsWith('moveheredir/movethisdir')).toEqual(true);
          expect(res.body.type).toEqual('directory');
        });
      expect(await fileVerifyCb('moveheredir', 'movethisdir')).toEqual(true);
    });

    it('can rename directory', async () => {
      await supertest(app)
        .post('/')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .send({ name: 'movethisdir1', type: 'directory' })
        .expect('Content-Type', /json/)
        .expect(200)
        .then((res) => {
          expect(res.body.refId.startsWith('movethisdir1')).toEqual(true);
          expect(res.body.type).toEqual('directory');
        });
      await supertest(app)
        .post('/movethisdir1')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .field('type', 'file')
        .field('name', 'moveme.jpg')
        .attach('file', `${__dirname}/resources/64-64.jpg`)
        .expect('Content-Type', /json/)
        .expect(200);
      expect(await fileVerifyCb('movethisdir1', 'moveme.jpg')).toEqual(true);
      await supertest(app)
        .put('/movethisdir1')
        .set('Authorization', `Bearer ${allAccessToken}`)
        .send({ target: '/moveheredir1' })
        .expect('Content-Type', /json/)
        .expect(200)
        .then((res) => {
          expect(res.body.refId.startsWith('moveheredir1')).toEqual(true);
          expect(res.body.type).toEqual('directory');
        });
      expect(await fileVerifyCb('moveheredir1')).toEqual(true);
    });
  });
};

module.exports = {
  getRoutes,
  postRoutes,
  putMetaRoutes,
  deleteRoutes,
  putRoutes,
};
*/