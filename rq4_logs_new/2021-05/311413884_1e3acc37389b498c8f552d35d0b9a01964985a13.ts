import supertest from "supertest";
import app from "../../app";
import { connection } from "../../db";

const userInfo = {
  username: "foo",
  email: "foo@bar.com",
  password: "what1234",
};

const serverInfo = {
  name: "foobar",
};

let accessToken: string;

let serverId: number;

let roomId: number;

let accessToken2: string;

afterAll(async () => {
  await connection.migrate.rollback();
  await connection.destroy();
});

beforeAll(async () => {
  await connection.migrate.latest();
  const user = await supertest(app)
    .post("/api/v1/auth/register")
    .send(userInfo);
  accessToken = user.body.accessToken;

  const user2 = await supertest(app)
    .post("/api/v1/auth/register")
    .send({
      ...userInfo,
      email: "what@what.com",
    });
  accessToken2 = user2.body.accessToken;

  const server = await supertest(app)
    .post("/api/v1/servers")
    .send(serverInfo)
    .set("Authorization", `Bearer ${accessToken}`);
  serverId = server.body.server.id;

  const room = await supertest(app)
    .post(`/api/v1/servers/${serverId}/rooms`)
    .send({
      name: "foobar",
    })
    .set("Authorization", `Bearer ${accessToken}`);
  roomId = room.body.room.id;
});

describe("Get all messages", () => {
  it("should return 200", async () => {
    const messages = await supertest(app)
      .get(`/api/v1/servers/${serverId}/rooms/${roomId}/messages`)
      .expect(200);
    expect(messages.body.messages).toHaveLength(0);
  });
});

describe("Create message", () => {
  it("should respond with 201", async () => {
    await supertest(app)
      .post(`/api/v1/servers/${serverId}/rooms/${roomId}/messages`)
      .send({
        message: "foobar",
      })
      .set("Authorization", `Bearer ${accessToken}`)
      .expect(201);
  });

  // Unauthorized
  it("should respond with 401", async () => {
    await supertest(app)
      .post(`/api/v1/servers/${serverId}/rooms/${roomId}/messages`)
      .send({
        message: "foobar",
      })
      .expect(401);
  });

  // Invalid payload 400
  it("should respond with 400", async () => {
    await supertest(app)
      .post(`/api/v1/servers/${serverId}/rooms/${roomId}/messages`)
      .send({
        foobar: "foobar",
      })
      .set("Authorization", `Bearer ${accessToken}`)
      .expect(400);
  });

  // Not a member
  it("should respond with 401", async () => {
    await supertest(app)
      .post(`/api/v1/servers/${serverId}/rooms/${roomId}/messages`)
      .set("Authorization", `Bearer ${accessToken2}`)
      .send({
        message: "foobar",
      })
      .expect(401);
  });
});