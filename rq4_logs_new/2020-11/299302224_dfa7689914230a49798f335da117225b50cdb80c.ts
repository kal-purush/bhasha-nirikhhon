import { FastifyInstance } from 'fastify';
import { CreateMessage, Message } from '../types/message';

export const messageRepository = ({ database: { query, firstRow } }: FastifyInstance) => ({
  create: ({ userId, content }: CreateMessage, _query = query) =>
    _query<Message>(
      `insert into message ( user_id, content )
        values ( $1, $2 )
        returning id, user_id as "userId", content, created_at as "createdAt", updated_at as "updatedAt"`,
      [userId, content],
    ).then(firstRow),
});