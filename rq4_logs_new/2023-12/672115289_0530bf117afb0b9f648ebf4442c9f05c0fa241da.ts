import { Application } from 'express';
import request from 'supertest';
import mongoose from 'mongoose';

import createApp from '../../../app';
import dbConnect from '../../../config/mongo';

describe('PATCH /bets/:id', () => {
  let app: Application;
  const betId: string = '658728515ce1200d553447c3';
  const invalidBetId: string = '0000fc00f000b00ea1700b00';

  beforeAll(async () => {
    await dbConnect();
  });

  beforeEach(() => {
    app = createApp();
  });

  it('Should return the status updated successfully', async () => {
    const response = await request(app)
      .patch(`/api/bets/${betId}`)
      .send({ status: 'win' })
      .expect(200);

    expect(response.body.message).toBe('Bet Status Updated');
  });

  it('Should return invalid status', async () => {
    const response = await request(app)
      .patch(`/api/bets/${betId}`)
      .send({ status: 'InvalidStatus' })
      .expect(400);

    expect(response.body.message).toBe('Invalid bet status');
  });

  it('Should return invalid Id', async () => {
    await request(app)
      .patch(`/api/bets/${invalidBetId}`)
      .send({ status: 'newStatus' })
      .expect(400);
  });

  afterAll(async () => {
    await mongoose.disconnect();
  });
});