import request from 'supertest';
import app from '../../app'; // Adjust this import path to where your Express app is defined.

describe('POST /user/login', () => {
  it('should authenticate user with valid credentials', async () => {
    const response = await request(app)
      .post('/user/login') // Adjusted to match the mounted route path
      .send({
        username: 'hrUser',
        password: 'password',
      });

    expect(response.statusCode).toBe(200);
    expect(response.body).toHaveProperty('user');
    // Assuming your successful login response includes a user object.
    // You might also want to check for the presence of set-cookie headers if cookies are being set.
  });

  it('should reject authentication with invalid credentials', async () => {
    const response = await request(app)
      .post('/user/login') // Adjusted to match the mounted route path
      .send({
        username: 'wrongUser',
        password: 'wrongPassword',
      });

    expect(response.statusCode).toBe(400); // Using 400 as per your login function's response for invalid credentials
  });
});