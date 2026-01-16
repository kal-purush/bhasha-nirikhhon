// Mock all middlewares used in app.ts BEFORE imports
jest.mock('../middleware/auth', () => ({
  isAuthenticated: (req: any, res: any, next: any) => next(),
  isClient: (req: any, res: any, next: any) => next(),
  isProvider: (req: any, res: any, next: any) => next(),
}));

import express from 'express';
import request from 'supertest';
import prisma from '../app';
import router from '../routes/client/signup';
import { globalErrorHandler } from '../middleware/errorHandler';

// Mock prisma.client.create and prisma.client.findUnique
const mockCreate = (prisma as any).client.create = jest.fn();
const mockFindUnique = (prisma as any).client.findUnique = jest.fn();
mockCreate.mockImplementation(async (data: any) => ({
  id: 'client1',
  email: data.data.email,
  fullName: data.data.fullName,
  phone: data.data.phone,
  password: data.data.password,
}));
mockFindUnique.mockImplementation(async ({ where }: any) => {
  if (where.email === 'test@client.com') {
    return { id: 'client1', email: 'test@client.com', fullName: 'Test Client', phone: '1234567890', password: 'password' };
  }
  return null;
});

const app = express();
app.use(express.json());
app.use('/client/signup', (req, res, next) => { req.login = () => {}; next(); }, router);
// Attach error handler middleware
app.use(globalErrorHandler);

describe('POST /client/signup', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockCreate.mockResolvedValue({ id: 'client1', email: 'test@client.com', fullName: 'Test Client' });
  });

  it('should create a new client and return user info', async () => {
    await request(app)
      .post('/client/signup')
      .send({ email: 'test@client.com', password: 'password', fullName: 'Test Client', phone: '1234567890' })
      .expect(201)
      .expect(res => {
        expect(res.body).toMatchObject({ id: 'client1', email: 'test@client.com', fullName: 'Test Client' });
      });
    expect(mockCreate).toHaveBeenCalled();
  });

  it('should fail with invalid input', async () => {
    await request(app)
      .post('/client/signup')
      .send({ email: '', password: '', fullName: '' })
      .expect(400);
  });
});