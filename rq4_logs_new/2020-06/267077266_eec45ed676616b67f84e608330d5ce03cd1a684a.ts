import { Message } from 'node-nats-streaming';
import { Listener, Subjects, TicketCreatedEvent } from '@gravitaz/common';
import { QUEUE_GROUP_NAME } from '../queue-group-name';
import { Ticket } from '../../models/ticket';

export class TicketCreatedListener extends Listener<TicketCreatedEvent> {
  readonly subject = Subjects.TicketCreated;
  readonly queueGroupName = QUEUE_GROUP_NAME;
  async onMessage(
    data: TicketCreatedEvent['data'],
    msg: Message
  ): Promise<void> {
    const { id, title, price } = data;
    const ticket = Ticket.build({
      id,
      title,
      price,
    });
    await ticket.save();
    msg.ack();
  }
}