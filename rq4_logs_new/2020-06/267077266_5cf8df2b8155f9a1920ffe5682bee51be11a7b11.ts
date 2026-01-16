import request from 'supertest';
import { app } from '../../app';
import { Order } from '../../models/orders';
import { natsWrapper } from '../../nats-wrapper';
import { Subjects, OrderStatus } from '@gravitaz/common';
import { Types as MongooseTypes } from 'mongoose';

it('has a route handler listening to /api/orders for post requests', async () => {
  const response = await request(app).post('/api/payments').send({});

  expect(response.status).not.toEqual(404);
});

it('can not be accessed if the user is not signed in', async () => {
  await request(app).post('/api/payments').send({}).expect(401);
});

it('can be accessed if the user is signed in', async () => {
  let response = await request(app)
    .post('/api/payments')
    .set('Cookie', global.signin())
    .send({});

  expect(response.status).not.toEqual(401);
});

it('returns an error if an invalid order is provided', async () => {
  await request(app)
    .post('/api/payments')
    .set('Cookie', global.signin())
    .send({ orderId: '', token: 'abc' })
    .expect(400);
});

it('returns an error if an invalid token is provided', async () => {
  await request(app)
    .post('/api/payments')
    .set('Cookie', global.signin())
    .send({ orderId: new MongooseTypes.ObjectId().toHexString(), token: '' })
    .expect(400);
});

it('Returns an error if the order is cancelled', async () => {
  const orderId = new MongooseTypes.ObjectId().toHexString();
  const user = {
    id: new MongooseTypes.ObjectId().toHexString(),
    email: 'test@test.com',
  };

  // create a fake order
  const order = Order.build({
    id: orderId,
    version: 0,
    status: OrderStatus.Cancelled,
    userId: user.id,
    price: 100,
  });
  await order.save();

  await request(app)
    .post('/api/payments')
    .set('Cookie', global.signin(user))
    .send({ orderId: orderId, token: 'ABC' })
    .expect(400);
});

it('Returns an error if the order is owned by a different user', async () => {
  const orderId = new MongooseTypes.ObjectId().toHexString();

  // create a fake order
  const order = Order.build({
    id: orderId,
    version: 0,
    status: OrderStatus.Created,
    userId: new MongooseTypes.ObjectId().toHexString(),
    price: 100,
  });
  await order.save();

  await request(app)
    .post('/api/payments')
    .set('Cookie', global.signin())
    .send({ orderId: orderId, token: 'ABC' })
    .expect(401);
});

it('Returns success if the order is created, user is same and inputs OK', async () => {
  const orderId = new MongooseTypes.ObjectId().toHexString();
  const user = {
    id: new MongooseTypes.ObjectId().toHexString(),
    email: 'test@test.com',
  };

  // create a fake order
  const order = Order.build({
    id: orderId,
    version: 0,
    status: OrderStatus.Created,
    userId: user.id,
    price: 100,
  });
  await order.save();

  await request(app)
    .post('/api/payments')
    .set('Cookie', global.signin(user))
    .send({ orderId: orderId, token: 'ABC' })
    .expect(201);
});