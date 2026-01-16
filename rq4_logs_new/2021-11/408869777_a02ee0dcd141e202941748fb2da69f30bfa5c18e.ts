import request from 'supertest';
import app from '../src/app';
import { prisma } from '../prisma/prismaClient';

const sampleMediaIcon = {
  name: 'Discord',
  iconUrl: 'https://svgshare.com/i/btZ.svg',
};

let idMediaIcon: string;

describe('MEDIA ICON ROUTES', () => {
  it('should create a new mediaIcon 🧪 /mediaicons', async () => {
    const res = await request(app)
      .post('/api/mediaicons')
      .send(sampleMediaIcon)
      .expect(201)
      .expect('Content-Type', /json/);

    idMediaIcon = res.body.id;

    expect(res.body).toHaveProperty('name', sampleMediaIcon.name);
    expect(res.body).toHaveProperty('iconUrl', sampleMediaIcon.iconUrl);
  });

  it('should get the mediaicons list 🧪 /mediaicons', async () => {
    const res = await request(app)
      .get('/api/mediaicons')
      .expect(200)
      .expect('Content-Type', /json/);

    expect(Array.isArray(res.body)).toBe(true);
  });

  it('should get the mediaicons with id 🧪 /mediaicons/:id', async () => {
    const res = await request(app)
      .get(`/api/mediaicons/${idMediaIcon}`)
      .expect(200)
      .expect('Content-Type', /json/);

    expect(res.body).toHaveProperty('name', sampleMediaIcon.name);
    expect(res.body).toHaveProperty('iconUrl', sampleMediaIcon.iconUrl);
  });

  it(`should update the created mediaicons title 🧪 /api/mediaicons/:id`, async () => {
    await request(app)
      .put(`/api/mediaicons/${idMediaIcon}`)
      .send({
        name: 'Discord',
      })
      .expect(204);

    const res = await request(app).get(`/api/mediaicons/${idMediaIcon}`);

    expect(res.body).toHaveProperty('name', sampleMediaIcon.name);
    expect(res.body).toHaveProperty('iconUrl', sampleMediaIcon.iconUrl);
  });
  it(`should delete the created MediaIcon 🧪 /api/mediaicons/:id`, async () => {
    await request(app).delete(`/api/mediaicons/${idMediaIcon}`).expect(204);
  });
});

afterAll(async () => {
  await prisma.$disconnect();
});