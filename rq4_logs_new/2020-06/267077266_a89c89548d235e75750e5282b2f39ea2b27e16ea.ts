import {
  PaymentCreatedEvent,
  Listener,
  Subjects,
  OrderStatus,
} from '@gravitaz/common';
import { Message } from 'node-nats-streaming';
import { QUEUE_GROUP_NAME } from '../queue-group-name';
import { Order } from '../../models/order';

export class PaymentCreatedListener extends Listener<PaymentCreatedEvent> {
  readonly subject = Subjects.PaymentCreated;
  queueGroupName = QUEUE_GROUP_NAME;
  async onMessage(
    data: PaymentCreatedEvent['data'],
    msg: Message
  ): Promise<void> {
    const { orderId } = data;
    const order = await Order.findById(orderId);
    if (!order) {
      throw new Error('Order not found');
    }
    order.set({ status: OrderStatus.Complete });
    await order.save();
    try {
      msg.ack();
    } catch (err) {
      console.log('Unable to acknowledge message', msg);
      console.log('Message subject', msg && msg.getSubject());
      console.error(err);
    }
  }
}