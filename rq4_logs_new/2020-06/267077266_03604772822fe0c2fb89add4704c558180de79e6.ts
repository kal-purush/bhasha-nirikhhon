import { Message } from 'node-nats-streaming';
import {
  Listener,
  Subjects,
  TicketUpdatedEvent,
  ResourceNotFoundError,
} from '@gravitaz/common';
import { QUEUE_GROUP_NAME } from '../queue-group-name';
import { Ticket } from '../../models/ticket';

export class TicketUpdatedListener extends Listener<TicketUpdatedEvent> {
  readonly subject = Subjects.TicketUpdated;
  readonly queueGroupName = QUEUE_GROUP_NAME;
  async onMessage(
    data: TicketUpdatedEvent['data'],
    msg: Message
  ): Promise<void> {
    const { id, version, title, price } = data;
    const ticket = await Ticket.findOne({
      _id: id,
      version: version - 1,
    });
    if (!ticket) {
      throw new ResourceNotFoundError(`Invalid ticket ${id} in event`);
    }
    ticket.set({ ticket, price });
    await ticket.save();
    msg.ack();
  }
}