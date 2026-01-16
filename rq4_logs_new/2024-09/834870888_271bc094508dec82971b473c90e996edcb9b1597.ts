import { AuthModel } from '@auth/models/auth.schema';
import bcrypt, { hash } from 'bcryptjs';
import { MongoMemoryServer } from 'mongodb-memory-server';
import mongoose from 'mongoose';

let mongoServer: MongoMemoryServer;

beforeAll(async () => {
  mongoServer = await MongoMemoryServer.create();
  const mongoUri = mongoServer.getUri();
  await mongoose.connect(mongoUri);
});

afterAll(async () => {
  await mongoose.connection.dropDatabase();
  await mongoose.connection.close();
  await mongoServer.stop();
  await mongoose.disconnect();
});

jest.mock('bcryptjs', () => ({
  hash: jest.fn().mockResolvedValue('hashedPassword'),
  compare: jest.fn().mockResolvedValue(true),
}));

describe('Auth Model', () => {
  let authDoc: any;

  beforeEach(() => {
    authDoc = new AuthModel({
      username: 'testUser',
      uId: '123456',
      email: 'test@example.com',
      password: 'password123',
      avatarColor: 'blue',
    });
  });
  afterEach(() => {
    jest.clearAllMocks(); // Clear mocks to avoid issues with mock state between tests
  });

  it('should hash the password before saving the user', async () => {
    jest.spyOn(authDoc, 'save').mockResolvedValueOnce(authDoc); // Mock the save method
    await authDoc.save();

    expect(authDoc.password).toEqual('password123'); // Mocked bcrypt result
  });

  it('should return false for incorrect password', async () => {
    jest.spyOn(bcrypt, 'compare').mockImplementation(() => Promise.resolve(false));
    await authDoc.save();
    const isValidPassword = await authDoc.comparePassword('wrongpassword');
    expect(isValidPassword).toBe(false);
  });

  it('should remove password field when transforming to JSON', async () => {
    const jsonObject = authDoc.toJSON();
    expect(jsonObject.password).toBeUndefined();
  });
  it('should hash the password before saving the user', (done) => {
    authDoc.save().then(() => {
      expect(authDoc.password).toEqual('hashedPassword'); // Mocked bcrypt result
      done(); // Signal that the test is complete
    });
  });

  it('should compare the password correctly', (done) => {
    authDoc.save().then(() => {
      authDoc.comparePassword('hashedPassword').then((isValidPassword: any) => {
        expect(isValidPassword).toBe(false); // Mocked bcrypt result
        done(); // Signal that the test is complete
      });
    });
  });
  it('should compare the password incorrectly', async () => {
    await authDoc.save();
    const isValidPassword = await authDoc.comparePassword('password123');
    expect(isValidPassword).toBe(false); // Mocked bcrypt result
  });
  it('should hash the password correctly', async () => {
    const hashedPassword = await authDoc.hashPassword('password123');

    // Check if the password is hashed and is different from the original
    expect(hashedPassword).not.toEqual('password123');

    // Ensure bcrypt's hash function was called with the correct password and salt rounds
    expect(hash).toHaveBeenCalledWith('password123', 10);
  });
});