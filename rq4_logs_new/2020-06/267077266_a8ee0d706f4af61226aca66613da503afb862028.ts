import nats, { Message } from 'node-nats-streaming';
import { randomBytes } from 'crypto';

console.clear();
const stan = nats.connect('ticketing', randomBytes(4).toString('hex'), {
  url: 'http://ticketing.dev:4222',
});

stan.on('connect', () => {
  console.log('Listener connected to NATS');

  const subscriptionOptions = stan.subscriptionOptions().setManualAckMode(true);

  const subscription = stan.subscribe(
    'ticket:created',
    'orders-service-queue-group'
  );

  subscription.on('message', (msg: Message) => {
    const data = msg.getData();
    if (typeof data === 'string') {
      console.log(
        `Received event# ${msg.getSequence()}, data: `,
        JSON.parse(data)
      );
    }
  });
});