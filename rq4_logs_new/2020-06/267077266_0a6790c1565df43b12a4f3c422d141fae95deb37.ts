import request from 'supertest';
import { app } from '../../app';
import { Order } from '../../models/orders';
import { natsWrapper } from '../../nats-wrapper';
import { Subjects, OrderStatus } from '@gravitaz/common';
import { Types as MongooseTypes } from 'mongoose';
import Stripe from 'stripe';

const stripe = new Stripe(process.env.STRIPE_KEY!, {
  apiVersion: '2020-03-02',
  typescript: true,
});

it('returns a 201 with valid inputs and creates a valid stripe charge', async () => {
  const orderId = new MongooseTypes.ObjectId().toHexString();
  const user = {
    id: new MongooseTypes.ObjectId().toHexString(),
    email: 'test@test.com',
  };
  const price = Math.floor(Math.random() * 100000);

  // create a fake order
  const order = Order.build({
    id: orderId,
    version: 0,
    status: OrderStatus.Created,
    userId: user.id,
    price: price,
  });
  await order.save();

  await request(app)
    .post('/api/payments')
    .set('Cookie', global.signin(user))
    .send({ orderId: orderId, token: 'tok_visa' })
    .expect(201);

  const stripeCharges = await stripe.charges.list({ limit: 50 });
  const stripeCharge = stripeCharges.data.find((charge) => {
    return charge.amount === price * 100;
  });
  expect(stripeCharge).toBeDefined();
});