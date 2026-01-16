import amqp from 'amqplib';
import { MatchRequest } from './types';

const QUEUE_NAME = 'matching_service_queue';

const HOST = '127.0.0.1';
const PORT = '5672';

export async function findMatch(request: MatchRequest) {
  const connection = await amqp.connect(`amqp://${HOST}:${PORT}`);
  const channel = await connection.createChannel();
  const q = await channel.assertQueue('', { exclusive: true });

  const correlationId = generateUuid();

  const consumerPromise = new Promise<string>((resolve) => {
    channel.consume(
      q.queue,
      function(msg) {
        if (!msg) {
          console.log('No message found on consume, dropping packet');
          return;
        }
        if (msg.properties.correlationId != correlationId) {
          console.log('Request id does not match, dropping packet');
        }
        connection.close();
        resolve(msg.content.toString());
      },
      {
        noAck: true
      }
    );
  });

  channel.sendToQueue(
    QUEUE_NAME,
    Buffer.from(JSON.stringify(request)),
    {
      correlationId: correlationId,
      replyTo: q.queue
    }
  );

  return consumerPromise;
}

function generateUuid() {
  return Math.random().toString() +
         Math.random().toString() +
         Math.random().toString();
}