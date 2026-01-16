import { Pool, PoolClient } from "pg";
import { PutMessageActionBody } from "../../types/message-actions.js";
import { httpErrors } from "@fastify/sensible";
import {
  MessagingEventType,
  newMessagingEventLogger,
} from "../messages/event-logger.js";
import { FastifyBaseLogger } from "fastify";

export async function updateMessageAction(params: {
  userId: string;
  body: PutMessageActionBody;
  pool: Pool;
  logger: FastifyBaseLogger;
}): Promise<void> {
  const { userId, body, pool, logger } = params;
  const client = await pool.connect();
  try {
    await ensureMessageExists({ userId, messageId: body.messageId, client });
    await setMessageReadStatus({ client, pool, body, userId, logger });
  } finally {
    client.release();
  }
}

async function ensureMessageExists(params: {
  userId: string;
  client: PoolClient;
  messageId: string;
}): Promise<void> {
  const existanceCheckQueryResult = await params.client.query<{
    exists: boolean;
  }>(
    `
          select exists(
            select * from messages m 
            join users u on (u.user_profile_id = $1 or u.id::text = $1)
            where (u.id::text = $1 or u.user_profile_id = $1) and m.id = $2
            limit 1
          )
        `,
    [params.userId, params.messageId],
  );

  if (!existanceCheckQueryResult.rows.at(0)?.exists) {
    throw httpErrors.notFound("message not found for user");
  }
}

async function setMessageReadStatus(params: {
  client: PoolClient;
  body: PutMessageActionBody;
  userId: string;
  pool: Pool;
  logger: FastifyBaseLogger;
}): Promise<void> {
  const updateQueryResult = await params.client.query<{ isSeen: boolean }>(
    `
    with message_selection as(
        select m.id from messages m
        join users u on (u.user_profile_id = $1 or u.id::text = $1)
        where 
            (u.id::text = $1 or u.user_profile_id = $1)
            and is_delivered = true 
            and m.id = $2
            and $3 != m.is_seen
    )
    update messages set is_seen = $3
    where id = any(select id from message_selection)
    returning is_seen as "isSeen", id
    `,
    [params.userId, params.body.messageId, params.body.isSeen],
  );

  if (updateQueryResult.rowCount) {
    const eventLogger = newMessagingEventLogger(params.pool, params.logger);
    const eventType = params.body.isSeen
      ? MessagingEventType.citizenSeenMessage
      : MessagingEventType.citizenUnseenMessage;

    eventLogger.log(eventType, [{ messageId: params.body.messageId }]);
  }
}