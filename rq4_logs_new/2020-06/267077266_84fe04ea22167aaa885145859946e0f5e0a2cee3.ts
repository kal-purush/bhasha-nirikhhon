import { ExpirationCompleteListener } from '../expiration-complete-listener';
import { natsWrapper } from '../../../nats-wrapper';
import { OrderStatus, ExpirationCompleteEvent } from '@gravitaz/common';
import { Types as MongooseTypes } from 'mongoose';
import { Message } from 'node-nats-streaming';
import { Ticket } from '../../../models/ticket';
import { Order } from '../../../models/order';

const setup = async () => {
  const listener = new ExpirationCompleteListener(natsWrapper.client);
  const ticket = Ticket.build({
    id: new MongooseTypes.ObjectId().toHexString(),
    title: 'concert',
    price: 20,
  });
  await ticket.save();
  const order = Order.build({
    ticket,
    status: OrderStatus.Created,
    expiresAt: new Date(),
    userId: 'ABC',
  });
  await order.save();

  const data: ExpirationCompleteEvent['data'] = {
    orderId: order.id,
  };
  // create a fake message
  // @ts-ignore
  const msg: Message = {
    ack: jest.fn(),
  };

  return { listener, data, msg, ticket, order };
};

it('cancels an order when a expiration:complete message is received', async () => {
  const { listener, data, msg, ticket, order } = await setup();

  await listener.onMessage(data, msg);

  const cancelledOrder = await Order.findById(order.id);

  expect(cancelledOrder?.status).toBe(OrderStatus.Cancelled);
  expect(msg.ack).toHaveBeenCalled();

  expect(natsWrapper.client.publish).toHaveBeenCalled();

  const orderCancelledEvent = JSON.parse(
    (natsWrapper.client.publish as jest.Mock).mock.calls[0][1]
  );

  expect(orderCancelledEvent.id).toBe(order.id);
  expect(orderCancelledEvent.ticket.id).toBe(ticket.id);
  expect(orderCancelledEvent.version).toBeGreaterThan(0);
});

it('does not cancel a completed order when a expiration:complete message is received', async () => {
  const { listener, data, msg, ticket, order } = await setup();

  order.set({ status: OrderStatus.Complete });
  await order.save();

  await listener.onMessage(data, msg);

  const completedOrder = await Order.findById(order.id);

  expect(completedOrder?.status).toBe(OrderStatus.Complete);
  expect(msg.ack).toHaveBeenCalled();

  expect(natsWrapper.client.publish).not.toHaveBeenCalled();
});