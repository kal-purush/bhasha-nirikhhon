import { isNativeError } from "node:util/types";
import { httpErrors } from "@fastify/sensible";
import {
  type LoggingError,
  toLoggingError,
} from "@ogcio/fastify-logging-wrapper";
import type { FastifyBaseLogger } from "fastify";
import { isHttpError } from "http-errors";
import type { Pool, PoolClient } from "pg";
import { EmailTransport } from "../providers/email/email-transport.js";
import { type ServiceError, utils } from "../../utils.js";
import { getProfileSdk } from "../../utils/authentication-factory.js";
import { ProfileSdkFacade, getUserProfiles } from "../users/shared-users.js";
import {
  type MessagingEventLogger,
  MessagingEventType,
  newMessagingEventLogger,
} from "../messages/event-logger.js";
import { JobTypes } from "../../types/jobs.js";
import { getPrimaryProvider } from "../providers/provider-service.js";
import { EmailProvider, SmsProvider } from "../../types/providers.js";
import { SmsTransport } from "../providers/sms/sms-transport.js";

type ScheduledMessageStatus = "pending" | "working" | "failed" | "delivered";

const statusWorking: ScheduledMessageStatus = "working";
const statusDelivered: ScheduledMessageStatus = "delivered";
const statusFailed: ScheduledMessageStatus = "failed";

type RunningJob = {
  jobId: string;
  userId: string;
  type: string;
  status: ScheduledMessageStatus;
  organizationId: string;
};

type MessageToDeliverWoAttachments = {
  transports?: string[];
  subject: string;
  excerpt: string;
  body: string;
};

type MessageToDeliver = MessageToDeliverWoAttachments & {
  attachmentIds: string[] | undefined;
};

export const executeJob = async (params: {
  pool: Pool;
  logger: FastifyBaseLogger;
  jobId: string;
  token: string;
}) => {
  const eventLogger = newMessagingEventLogger(params.pool, params.logger);
  const job = await setJobAsRunning({
    eventLogger,
    pool: params.pool,
    job: { id: params.jobId, token: params.token },
  });

  switch (job.type) {
    case JobTypes.Message:
      await processMessageJob({
        job,
        eventLogger,
        pool: params.pool,
        logger: params.logger,
      });
      break;
    default:
      params.logger.warn(job, "Job has unrecognized type");
  }
};

async function setJobAsRunning(params: {
  eventLogger: MessagingEventLogger;
  pool: Pool;
  job: { id: string; token: string };
}): Promise<RunningJob> {
  const { pool, job, eventLogger } = params;
  const client = await pool.connect();
  let runningJob: RunningJob | undefined;
  try {
    client.query("BEGIN");

    const jobStatusResult = await client.query<{
      status: ScheduledMessageStatus;
      entityId: string;
      organizationId: string;
    }>(
      `
        SELECT
          coalesce(delivery_status, 'pending') as "status",
          job_id as "entityId",
          organisation_id as "organizationId"
        FROM jobs WHERE id = $1
        AND case when delivery_status is not null then delivery_status != $2 else true end
        AND job_token = $3
    `,
      [job.id, statusDelivered, job.token],
    );

    const jobResult = jobStatusResult.rows.at(0);

    if (!jobResult) {
      await eventLogger.log(MessagingEventType.deliverMessageError, [
        { messageId: "" }, // job id error field?
      ]);
      throw httpErrors.notFound("job doesn't exist");
    }

    await eventLogger.log(MessagingEventType.deliverMessagePending, [
      { messageId: jobResult.entityId }, // job id error field?
    ]);

    if (jobResult.status === statusWorking) {
      throw httpErrors.badRequest("job is already in progress");
    }

    const updateResult = await client.query<RunningJob>(
      `
        UPDATE jobs SET delivery_status = $1
        WHERE id = $2
        returning 
        user_id as "userId",
        job_type as "type",
        job_id as "jobId",
        organisation_id as "organizationId"
    `,
      [statusWorking, job.id],
    );

    runningJob = updateResult.rows.at(0);
    if (!runningJob) {
      throw httpErrors.notFound("Not able to find job to update");
    }
    client.query("COMMIT");
  } catch (err) {
    client.query("ROLLBACK");

    await eventLogger.log(MessagingEventType.deliverMessageError, [
      { messageId: "" },
    ]);
    if (isHttpError(err)) {
      throw err;
    }

    throw httpErrors.createError(500, "Failed fetching/updating job", {
      parent: err,
    });
  } finally {
    client.release();
  }

  if (!runningJob.userId || !runningJob.type) {
    await eventLogger.log(MessagingEventType.deliverMessageError, [
      { messageId: runningJob?.jobId || "" },
    ]);
    throw httpErrors.internalServerError(
      `job row with id ${runningJob.jobId} missing critical fields`,
    );
  }

  return runningJob;
}

async function processMessageJob(params: {
  job: RunningJob;
  eventLogger: MessagingEventLogger;
  pool: Pool;
  logger: FastifyBaseLogger;
}) {
  const { job, eventLogger, pool, logger } = params;
  const deliveryError = await deliverMessage({
    pool,
    jobId: job.jobId,
    recipientUserId: job.userId,
    eventLogger,
    organizationId: job.organizationId,
    messageId: job.jobId,
    logger,
  });

  return updateJobStatus({ ...params, deliveryError });
}

async function deliverMessage(params: {
  pool: Pool;
  messageId: string;
  recipientUserId: string;
  eventLogger: MessagingEventLogger;
  organizationId: string;
  logger: FastifyBaseLogger;
  jobId: string;
}): Promise<LoggingError | undefined> {
  const { messageId, pool, recipientUserId, logger, jobId, eventLogger } =
    params;
  const client = await pool.connect();
  try {
    // The update won't be applied until the COMMIT query is run
    const messageData = await getMessageToDeliver({
      messageId,
      recipientUserId,
      client,
    });

    const transportErrors = await sendMessageToTransports({
      ...params,
      client,
      messageData,
    });

    for (const err of transportErrors) {
      logger.error({ error: err.error }, err.msg);
    }

    const firstError = transportErrors.filter((err) => err.critical).at(0);
    let loggingError: LoggingError | undefined;
    if (firstError) {
      loggingError = toLoggingError(
        httpErrors.internalServerError(firstError.msg),
      );
    }
    if (!loggingError) {
      await eventLogger.log(MessagingEventType.deliverMessage, [
        { messageId: jobId },
      ]);
      return undefined;
    }

    return loggingError;
  } catch (err) {
    return toLoggingError(
      httpErrors.createError(500, "Error sending message", { parent: err }),
    );
  } finally {
    client.release();
  }
}

async function getMessageToDeliver(params: {
  messageId: string;
  recipientUserId: string;
  client: PoolClient;
}) {
  const { messageId, recipientUserId, client } = params;
  const messageUpdateQueryResult = await client.query<
    MessageToDeliverWoAttachments & {
      attachmentId?: string;
    }
  >(
    `
    SELECT 
    m.preferred_transports AS "transports",
    m.excerpt,
    m.subject,
    CASE 
        WHEN m.rich_text <> '' THEN m.rich_text 
        ELSE m.plain_text 
    END AS "body",
    aid.attachment_id AS "attachmentId"
    FROM messages m
    LEFT JOIN (
        SELECT attachment_id, message_id 
        FROM attachments_messages
    ) aid
    ON m.id = aid.message_id
    WHERE m.id = $1;
  `,
    [messageId],
  );
  let messageData: MessageToDeliver | undefined;
  const attachmentIds = [];
  for (const row of messageUpdateQueryResult.rows) {
    if (row.attachmentId) {
      attachmentIds.push(row.attachmentId);
    }
    if (!messageData) {
      messageData = {
        body: row.body,
        excerpt: row.excerpt,
        subject: row.subject,
        transports: row.transports ? row.transports : undefined,
        attachmentIds: undefined,
      };
      continue;
    }
  }

  if (!messageData) {
    throw httpErrors.notFound(`failed to find message for id ${messageId}`);
  }
  messageData.attachmentIds =
    attachmentIds.length > 0 ? attachmentIds : undefined;

  return messageData;
}

async function updateJobStatus(params: {
  deliveryError: LoggingError | undefined;
  pool: Pool;
  job: { jobId: string; userId: string };
}): Promise<void> {
  const { pool, deliveryError, job } = params;
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    if (deliveryError) {
      await client.query(
        `
          UPDATE jobs SET delivery_status = $1
          WHERE job_id = $2 AND user_id = $3
        `,
        [statusFailed, job.jobId, job.userId],
      );
    } else {
      await setMessageAsDelivered({
        messageId: job.jobId,
        recipientUserId: job.userId,
        client,
      });
    }
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  }
}

async function setMessageAsDelivered(params: {
  messageId: string;
  recipientUserId: string;
  client: PoolClient;
}): Promise<void> {
  const { messageId, client } = params;
  const messageUpdateQueryResult = await client.query<
    MessageToDeliverWoAttachments & {
      attachmentId?: string;
    }
  >(
    `
    UPDATE messages m set 
      is_delivered = true,
      updated_at = now()
    WHERE m.id = $1
  `,
    [messageId],
  );

  if (messageUpdateQueryResult.rowCount === 0) {
    throw httpErrors.notFound(`failed to find message for id ${messageId}`);
  }

  await client.query(
    `
      UPDATE jobs SET delivery_status = $1
      WHERE job_id = $2 AND user_id = $3
    `,
    [statusDelivered, messageId, params.recipientUserId],
  );
}

async function sendMessageToTransports(params: {
  pool: Pool;
  organizationId: string;
  recipientUserId: string;
  messageId: string;
  messageData: MessageToDeliver;
  client: PoolClient;
  eventLogger: MessagingEventLogger;
  logger: FastifyBaseLogger;
}): Promise<ServiceError[]> {
  const {
    messageData,
    client,
    eventLogger,
    logger,
    organizationId,
    recipientUserId,
    messageId,
    pool,
  } = params;
  if (!messageData.transports) {
    return [];
  }
  const errors: ServiceError[] = [];
  let partialSuccessCount = 0;
  let fullSuccessCount = 0;
  const profileSdk = await getProfileSdk(logger, organizationId);
  const messageSdk = {
    selectUsers(ids: string[]) {
      return getUserProfiles(ids, client);
    },
  };
  const profileService = ProfileSdkFacade(profileSdk, messageSdk);

  try {
    const { data } = await profileService.selectUsers([recipientUserId]);
    const user = data?.at(0);
    if (!user) {
      throw httpErrors.notFound("no user profile found");
    }
    for (const transport of messageData.transports) {
      if (transport === "email") {
        if (!user.email) {
          await eventLogger.log(MessagingEventType.emailError, [
            {
              messageId,
              messageKey: "noEmail",
            },
          ]);
          partialSuccessCount++;
          continue;
        }
        if (!messageData.subject) {
          await eventLogger.log(MessagingEventType.emailError, [
            {
              messageId,
              messageKey: "noSubject",
            },
          ]);
          partialSuccessCount++;
          continue;
        }

        let providerId: string | undefined;
        try {
          let provider: EmailProvider | undefined;
          try {
            provider = await getPrimaryProvider({
              organisationId: organizationId,
              providerType: "email",
              pool,
            });
            if (!provider) {
              await eventLogger.log(MessagingEventType.emailError, [
                {
                  messageId,
                  messageKey: "noProvider",
                },
              ]);
              partialSuccessCount++;
              continue;
            }
          } catch (_e) {
            await eventLogger.log(MessagingEventType.emailError, [
              {
                messageId,
                messageKey: "noProvider",
              },
            ]);
            partialSuccessCount++;
            continue;
          }

          const mailTransport = new EmailTransport(provider);

          await mailTransport.sendMessage({
            recipientAddress: user.email,
            message: messageData,
          });

          fullSuccessCount++;
        } catch (err) {
          await eventLogger.log(MessagingEventType.emailError, [
            {
              messageId,
              messageKey: "failedToSend",
              details: JSON.stringify(err),
            },
          ]);
          errors.push({
            critical: false,
            error: {
              userId: recipientUserId,
              providerId,
              transportationSubject: messageData.subject,
              transportationBody: messageData.body,
            },
            msg: "failed to send email",
          });
        }
      } else if (transport === "sms") {
        if (!user.phone) {
          await eventLogger.log(MessagingEventType.smsError, [
            {
              messageId,
              messageKey: "noPhone",
            },
          ]);
          partialSuccessCount++;
          continue;
        }
        if (!messageData.excerpt || !messageData.subject) {
          await eventLogger.log(MessagingEventType.smsError, [
            {
              messageId,
              messageKey: "missingContent",
            },
          ]);
          partialSuccessCount++;
          continue;
        }

        let provider: SmsProvider | undefined;
        try {
          provider = await getPrimaryProvider({
            organisationId: organizationId,
            providerType: "sms",
            pool,
          });
          if (!provider) {
            await eventLogger.log(MessagingEventType.smsError, [
              {
                messageId,
                messageKey: "noProvider",
              },
            ]);
            partialSuccessCount++;
            continue;
          }
        } catch (_e) {
          await eventLogger.log(MessagingEventType.smsError, [
            {
              messageId,
              messageKey: "noProvider",
            },
          ]);
          partialSuccessCount++;
          continue;
        }

        const smsTranport = new SmsTransport(provider);

        try {
          await smsTranport.sendMessage({
            message: messageData,
            recipientAddress: user.phone,
          });
          fullSuccessCount++;
        } catch (err) {
          eventLogger.log(MessagingEventType.smsError, [
            {
              messageId,
            },
          ]);
          const msg = utils.isError(err) ? err.message : "failed to send sms";
          errors.push({
            critical: false,
            error: {
              userId: recipientUserId,
              transportationExcerpt: messageData.excerpt,
              transportationSubject: messageData.subject,
            },
            msg,
          });
        }
      }
    }
  } catch (err) {
    errors.push({
      critical: false,
      error: { err },
      msg: isNativeError(err)
        ? err.message
        : "failed to externally transport message",
    });
  }

  if (
    fullSuccessCount > 0 ||
    partialSuccessCount === messageData.transports.length
  ) {
    return errors;
  }

  errors.push({
    critical: true,
    msg: "Not been able to send to any transport",
    error: httpErrors.badGateway("Message has not been sent anywhere"),
  });

  return errors;
}