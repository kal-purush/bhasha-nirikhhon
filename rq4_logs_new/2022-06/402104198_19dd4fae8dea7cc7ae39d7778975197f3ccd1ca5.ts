/* eslint-disable import/no-extraneous-dependencies */
import supertest from 'supertest';
import { TestDB } from '@utils/tests/testDB';

import app from '@/app';

describe('POST /api/announcements', () => {
  const auth: {
    accessToken?: string;
  } = {};
  beforeAll(async () => {
    await TestDB.initDB();
    auth.accessToken = await TestDB.getTestUserToken();
  });

  afterAll(async () => {
    await TestDB.dropDB();
  });
  describe('Add new announcement', () => {
    it('Should add new announcement and returns correct response', async () => {
      const payload = {
        city: 'testCity',
        coordinates: { lat: 50.01787, lng: 22.22905 },
        description: 'testDescription',
      };
      await supertest(app)
        .post('/api/announcements')
        .set('Authorization', `Bearer ${auth.accessToken}`)
        .send(payload)
        .expect(201)
        .then((response) => {
          expect(response.body.success).toBe(true);
          expect(response.body.announcement._id).toBeDefined();
          expect(response.body.announcement.author.id).toBeDefined();
          expect(response.body.announcement.author.name).toBeDefined();
          expect(response.body.announcement.city).toBe('testCity');
          expect(response.body.announcement.description).toBe(
            'testDescription'
          );
          expect(response.body.announcement.coordinates.lat).toBe(50.01787);
          expect(response.body.announcement.coordinates.lng).toBe(22.22905);
          expect(response.body.announcement.status).toBe('open');
          expect(response.body.announcement.createdAt).toBeDefined();
          expect(response.body.announcement.updatedAt).toBeDefined();
        });
    });
  });
});