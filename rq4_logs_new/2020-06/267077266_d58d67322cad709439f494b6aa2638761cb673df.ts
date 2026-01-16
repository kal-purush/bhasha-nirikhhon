import { Listener, OrderCreatedEvent, Subjects } from '@gravitaz/common';
import { Message } from 'node-nats-streaming';
import { QUEUE_GROUP_NAME } from '../queue-group-name';
import { expirationQueue } from '../../queues/expiration-queue';

export class OrderCreatedListener extends Listener<OrderCreatedEvent> {
  readonly subject = Subjects.OrderCreated;
  readonly queueGroupName = QUEUE_GROUP_NAME;
  async onMessage(
    data: OrderCreatedEvent['data'],
    msg: Message
  ): Promise<void> {
    const delay = new Date(data.expiresAt).getTime() - new Date().getTime();
    console.log(`Expiring order in ${delay}ms`);
    await expirationQueue.add(
      {
        orderId: data.id,
      },
      {
        delay: 5000,
      }
    );

    msg.ack();
  }
}