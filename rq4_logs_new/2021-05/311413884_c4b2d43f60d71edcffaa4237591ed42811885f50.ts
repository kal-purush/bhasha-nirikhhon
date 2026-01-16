import supertest from "supertest";
import app from "../../app";
import { connection } from "../../db";

const userInfo = {
  username: "foo",
  email: "foo@bar.com",
  password: "what1234",
};

const userInfo2 = {
  username: "foo2",
  email: "foo@bar2.com",
  password: "what1234",
};

const serverInfo = {
  name: "foobar",
};

let accessToken: string;

let accessToken2: string;

let serverId: number;

let inviteCode: string;

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
    .send(userInfo2);
  accessToken2 = user2.body.accessToken;

  const { body } = await supertest(app)
    .post("/api/v1/servers")
    .send(serverInfo)
    .set("Authorization", `Bearer ${accessToken}`);
  serverId = body.server.id;

  const invite = await supertest(app)
    .get(`/api/v1/servers/${body.server.id}/invites`)
    .set("Authorization", `Bearer ${accessToken}`);
  inviteCode = invite.body.inviteCode;
});

afterEach(async () => {
  await connection.migrate.rollback();
  await connection.migrate.latest();
  const user = await supertest(app)
    .post("/api/v1/auth/register")
    .send(userInfo);
  accessToken = user.body.accessToken;

  const user2 = await supertest(app)
    .post("/api/v1/auth/register")
    .send(userInfo2);
  accessToken2 = user2.body.accessToken;

  const { body } = await supertest(app)
    .post("/api/v1/servers")
    .send(serverInfo)
    .set("Authorization", `Bearer ${accessToken}`);
  serverId = body.server.id;

  const invite = await supertest(app)
    .get(`/api/v1/servers/${body.server.id}/invites`)
    .set("Authorization", `Bearer ${accessToken}`);
  inviteCode = invite.body.inviteCode;
});

describe("Get all members", () => {
  it("should return an array with one member", async () => {
    const { body } = await supertest(app)
      .get(`/api/v1/servers/${serverId}/members`)
      .expect(200);
    expect(body.members).toHaveLength(1);
  });
});

describe("Get one member", () => {
  // Success
  it("should respond with 200", async () => {
    const { body } = await supertest(app)
      .get(`/api/v1/servers/${serverId}/members/1`)
      .set("Authorization", `Bearer ${accessToken}`)
      .expect(200);
    expect(body.member.id).toBe(1);
  });
  // Not found
  it("should respond with 404", async () => {
    await supertest(app)
      .get(`/api/v1/servers/${serverId}/members/400`)
      .set("Authorization", `Bearer ${accessToken}`)
      .expect(404);
  });
  // Unauthorized
  it("should respond with 401", async () => {
    await supertest(app)
      .get(`/api/v1/servers/${serverId}/members/400`)
      .expect(401);
  });
});

// Join/create
describe("Create member", () => {
  // Success
  it("should respond with 200", async () => {
    await supertest(app)
      .post(`/api/v1/servers/${serverId}/members`)
      .set("Authorization", `Bearer ${accessToken2}`)
      .send({
        inviteCode,
      })
      .expect(201);
  });

  // Validation error
  it("should respond with 400", async () => {
    await supertest(app)
      .post(`/api/v1/servers/${serverId}/members`)
      .set("Authorization", `Bearer ${accessToken2}`)
      .send({
        foo: "bar",
      })
      .expect(400);
    await supertest(app)
      .post(`/api/v1/servers/${serverId}/members`)
      .set("Authorization", `Bearer ${accessToken2}`)
      .send({
        foo: "12345678910",
      })
      .expect(400);
  });

  // Unauthorized
  it("should respond with 401", async () => {
    await supertest(app)
      .post(`/api/v1/servers/${serverId}/members`)
      .expect(401);
  });

  // Already a member
  it("should respond with 409", async () => {
    await supertest(app)
      .post(`/api/v1/servers/${serverId}/members`)
      .set("Authorization", `Bearer ${accessToken}`)
      .send({
        inviteCode,
      })
      .expect(409);
  });
});

// Delete
describe("Delete member", () => {
  // Success
  it("should return 204", async () => {
    const { body } = await supertest(app)
      .post(`/api/v1/servers/${serverId}/members`)
      .set("Authorization", `Bearer ${accessToken2}`)
      .send({
        inviteCode,
      });
    await supertest(app)
      .del(`/api/v1/servers/${serverId}/members`)
      .send({
        userId: body.member.userId,
      })
      .set("Authorization", `Bearer ${accessToken}`)
      .expect(204);
  });

  // Not the owner
  it("should return 401", async () => {
    const { body } = await supertest(app)
      .post(`/api/v1/servers/${serverId}/members`)
      .set("Authorization", `Bearer ${accessToken2}`)
      .send({
        inviteCode,
      });
    await supertest(app)
      .del(`/api/v1/servers/${serverId}/members`)
      .send({
        userId: body.member.userId,
      })
      .set("Authorization", `Bearer ${accessToken2}`)
      .expect(204);
  });

  // Unauthorized
  it("should return 401", async () => {
    await supertest(app).del(`/api/v1/servers/${serverId}/members`).expect(401);
  });
});