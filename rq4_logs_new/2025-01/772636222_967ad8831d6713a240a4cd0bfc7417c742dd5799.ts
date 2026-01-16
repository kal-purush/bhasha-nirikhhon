import type { Pool } from "pg";
import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import { processMessage } from "../../../services/messages/message-service.js";
import {
  DATABASE_TEST_URL_KEY,
  getPoolFromConnectionString,
} from "../../build-testcontainer-pg.js";
import { getMockBaseLogger } from "../../test-server-builder.js";
import { utils } from "../../../utils.js";
import { getCurrentUTCDate } from "../../../utils/date-times.js";
import {
  MessagingEventLogger,
  MessagingEventType,
} from "../../../services/messages/event-logger.js";
import { randomUUID } from "crypto";
import {
  getMessageEvent,
  listMessageEvents,
} from "../../../services/message-events/message-event-service.js";

let pool: Pool;
let messagingEventLogger: MessagingEventLogger;
const organizationId = "message-event-org";
beforeAll(() => {
  pool = getPoolFromConnectionString(process.env[DATABASE_TEST_URL_KEY]);
  messagingEventLogger = new MessagingEventLogger(pool, getMockBaseLogger());
});

afterAll(async () => {
  if (pool) {
    await pool.end();
  }
});

afterEach(async () => {
  await pool.query("TRUNCATE TABLE messaging_event_logs;");
  await pool.query("DELETE FROM messages where organisation_id = $1;", [
    organizationId,
  ]);
});

async function insertMockMessage(
  subject: string = "Sub",
): Promise<{ id: string; user_id: string; organisation_id: string }> {
  const valueArray = [
    false,
    randomUUID().substring(0, 11),
    subject,
    "Exc",
    "plain",
    "rich",
    "public",
    "en",
    utils.postgresArrayify(["lifeEvent"]),
    "threadName",
    organizationId,
    getCurrentUTCDate(),
  ];

  const values = valueArray.map((_, i) => `$${i + 1}`).join(", ");
  const insertQueryResult = await pool.query<{
    id: string;
    user_id: string;
    organisation_id: string;
  }>(
    `
              insert into messages(
                  is_delivered,
                  user_id,
                  subject,
                  excerpt,
                  plain_text,
                  rich_text,
                  security_level,
                  lang,
                  preferred_transports,
                  thread_name,
                  organisation_id,
                  scheduled_at
              ) values (${values})
              returning 
                id, user_id, organisation_id;
            `,
    valueArray,
  );

  return insertQueryResult.rows[0];
}

describe("Message Event Service - getEvent", () => {
  it("should process messages successfully", async () => {
    const message = await insertMockMessage();
    messagingEventLogger.log(MessagingEventType.createRawMessage, {
      messageId: message.id,
      organisationName: message.organisation_id,
      receiverFullName: message.user_id,
    });
    messagingEventLogger.log(MessagingEventType.deliverMessage, {
      messageId: message.id,
    });
    await messagingEventLogger.commit();
    const listed = await listMessageEvents({
      pagination: { offset: "0", limit: "100" },
      pool,
      organizationId: message.organisation_id,
    });

    expect(listed.totalCount).toBe(1);

    const single = await getMessageEvent({
      eventId: listed.data[0].id,
      organizationId: message.organisation_id,
      pool,
    });

    expect(single.length).toBe(2);

    expect(single[0].messageId).toBe(message.id);
    expect(single[1].messageId).toBe(message.id);
  });

  it("should handle errors if no event exists", async () => {
    await expect(
      getMessageEvent({
        eventId: randomUUID(),
        organizationId: randomUUID(),
        pool,
      }),
    ).rejects.toThrow("No message event found for the organization");
  });
});

describe("Message Event Service - listEvents", () => {
  it("should return empty list if no events", async () => {
    const listed = await listMessageEvents({
      pagination: { offset: "0", limit: "100" },
      pool,
      organizationId: organizationId,
    });

    expect(listed.totalCount).toBe(0);
    expect(listed.data.length).toBe(0);
  });

  it("should paginate events correctly", async () => {
    const createdMockMessages = [];
    for (let i = 0; i < 5; i++) {
      const message = await insertMockMessage(`sub-${i}`);
      messagingEventLogger.log(MessagingEventType.createRawMessage, {
        messageId: message.id,
        organisationName: message.organisation_id,
        receiverFullName: message.user_id,
      });
      createdMockMessages.push(message);
    }

    await messagingEventLogger.commit();
    const listed = await listMessageEvents({
      pagination: { offset: "0", limit: "100" },
      pool,
      organizationId: organizationId,
    });

    expect(listed.totalCount).toBe(createdMockMessages.length);
    expect(listed.data.map((d) => d.messageId).sort()).toStrictEqual(
      createdMockMessages.map((c) => c.id).sort(),
    );

    // first page
    const firstPage = await listMessageEvents({
      pagination: { offset: "0", limit: "2" },
      pool,
      organizationId: organizationId,
    });

    expect(firstPage.totalCount).toBe(createdMockMessages.length);
    expect(firstPage.data.length).toBe(2);
    expect(firstPage.data[0].id).toBe(listed.data[0].id);
    expect(firstPage.data[1].id).toBe(listed.data[1].id);

    // second page
    const secondPage = await listMessageEvents({
      pagination: { offset: "2", limit: "2" },
      pool,
      organizationId: organizationId,
    });

    expect(secondPage.totalCount).toBe(createdMockMessages.length);
    expect(secondPage.data.length).toBe(2);
    expect(secondPage.data[0].id).toBe(listed.data[2].id);
    expect(secondPage.data[1].id).toBe(listed.data[3].id);

    // third page, must return only one item because total 5
    const thirdPage = await listMessageEvents({
      pagination: { offset: "4", limit: "2" },
      pool,
      organizationId: organizationId,
    });

    expect(thirdPage.totalCount).toBe(createdMockMessages.length);
    expect(thirdPage.data.length).toBe(1);
    expect(thirdPage.data[0].id).toBe(listed.data[4].id);

    // overflow
    const overflow = await listMessageEvents({
      pagination: { offset: "6", limit: "2" },
      pool,
      organizationId: organizationId,
    });

    expect(overflow.totalCount).toBe(createdMockMessages.length);
    expect(overflow.data.length).toBe(0);
  });

  it("should find equal subject", async () => {
    const createdMockMessages = [];
    for (let i = 0; i < 5; i++) {
      const message = await insertMockMessage(`sub-bis-${i}`);
      messagingEventLogger.log(MessagingEventType.createRawMessage, {
        messageId: message.id,
        organisationName: message.organisation_id,
        receiverFullName: message.user_id,
      });
      createdMockMessages.push(message);
    }

    await messagingEventLogger.commit();
    const listed = await listMessageEvents({
      pagination: { offset: "0", limit: "100" },
      pool,
      organizationId: organizationId,
      search: `sub-bis-0`
    });

    expect(listed.totalCount).toBe(1);
    expect(listed.data[0].messageId).toBe(createdMockMessages[0].id);
  });

  it("should find like subject", async () => {
    const createdMockMessages = [];
    for (let i = 0; i < 5; i++) {
      const message = await insertMockMessage(`sub-another-${i}`);
      messagingEventLogger.log(MessagingEventType.createRawMessage, {
        messageId: message.id,
        organisationName: message.organisation_id,
        receiverFullName: message.user_id,
      });
      createdMockMessages.push(message);
    }

    await messagingEventLogger.commit();
    const listed = await listMessageEvents({
      pagination: { offset: "0", limit: "100" },
      pool,
      organizationId: organizationId,
      search: '-another-'
    });

    expect(listed.totalCount).toBe(5);
  });

  it("should find equal recipient", async () => {
    const createdMockMessages = [];
    for (let i = 0; i < 5; i++) {
      const message = await insertMockMessage(`sub-eq-rec-${i}`);
      messagingEventLogger.log(MessagingEventType.createRawMessage, {
        messageId: message.id,
        organisationName: message.organisation_id,
        receiverFullName: message.user_id,
      });
      createdMockMessages.push(message);
    }

    await messagingEventLogger.commit();
    const listed = await listMessageEvents({
      pagination: { offset: "0", limit: "100" },
      pool,
      organizationId: organizationId,
      search: createdMockMessages[0].user_id
    });

    expect(listed.totalCount).toBe(1);
    expect(listed.data[0].messageId).toBe(createdMockMessages[0].id);
  });

  it("should find like recipients", async () => {
    const createdMockMessages = [];
    for (let i = 0; i < 5; i++) {
      const message = await insertMockMessage(`sub-like-rec-${i}`);
      messagingEventLogger.log(MessagingEventType.createRawMessage, {
        messageId: message.id,
        organisationName: message.organisation_id,
        receiverFullName: `Something ${i} - Charlie Chaplin`,
      });
      createdMockMessages.push(message);
    }

    await messagingEventLogger.commit();
    const listed = await listMessageEvents({
      pagination: { offset: "0", limit: "100" },
      pool,
      organizationId: organizationId,
      search: '0 - Charlie'
    });

    expect(listed.totalCount).toBe(1);
  });

  it("should return last event status for each message with aggregated data", async () => {
    const message = await insertMockMessage(`sub-last`);
    messagingEventLogger.log(MessagingEventType.createRawMessage, {
      messageId: message.id,
      organisationName: message.organisation_id,
      receiverFullName: message.user_id,
    });
    messagingEventLogger.log(MessagingEventType.deliverMessage, {
      messageId: message.id,
    });

    await messagingEventLogger.commit();
    const listed = await listMessageEvents({
      pagination: { offset: "0", limit: "100" },
      pool,
      organizationId: organizationId,
    });

    expect(listed.totalCount).toBe(1);
    expect(listed.data[0].receiverFullName).toBe(message.user_id);
    expect(listed.data[0].eventStatus).toBe(
      MessagingEventType.deliverMessage.status,
    );
    expect(listed.data[0].eventType).toBe(
      MessagingEventType.deliverMessage.key,
    );
  });
});