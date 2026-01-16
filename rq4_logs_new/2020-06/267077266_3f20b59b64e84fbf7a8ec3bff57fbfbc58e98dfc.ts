import { TicketCreatedListener } from '../ticket-created-listener';
import { natsWrapper } from '../../../nats-wrapper';
import { TicketCreatedEvent } from '@gravitaz/common';
import { Types as MongooseTypes } from 'mongoose';
import { Message } from 'node-nats-streaming';
import { Ticket } from '../../../models/ticket';

const setup = async () => {
  // create an instance of the listener
  const listener = new TicketCreatedListener(natsWrapper.client);
  // create a fake data event

  const data: TicketCreatedEvent['data'] = {
    version: 0,
    id: new MongooseTypes.ObjectId().toHexString(),
    title: 'TicketTitle',
    price: 10,
    userId: new MongooseTypes.ObjectId().toHexString(),
  };
  // create a fake message
  // @ts-ignore
  const msg: Message = {
    ack: jest.fn(),
  };

  return { listener, data, msg };
};

it('creates and saves a ticket', async () => {
  const { listener, data, msg } = await setup();

  // call the onMessage function with data object + message object
  await listener.onMessage(data, msg);

  // write ssertions to make sure a ticket was created!
  const ticket = await Ticket.findById(data.id);

  expect(ticket).toBeDefined;
  expect(ticket?.title).toBe('TicketTitle');
});

it('acks the message', async () => {
  const { listener, data, msg } = await setup();
  // call the onMessage function with data object + message object
  await listener.onMessage(data, msg);

  // write ssertions to make sure an ack ws called!
  expect(msg.ack).toHaveBeenCalled();
});