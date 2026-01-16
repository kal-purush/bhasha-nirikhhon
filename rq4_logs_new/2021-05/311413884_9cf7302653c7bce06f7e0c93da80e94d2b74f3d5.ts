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

  const { body } = await supertest(app)
    .post("/api/v1/servers")
    .send(serverInfo)
    .set("Authorization", `Bearer ${accessToken}`);
  serverId = body.server.id;
});

describe("Create invite code", () => {
  it("should respond with 201", async () => {
    const { body } = await supertest(app)
      .get(`/api/v1/servers/${serverId}/invites`)
      .set("Authorization", `Bearer ${accessToken}`)
      .expect(201);
    expect(body.inviteCode).toBeTruthy();
  });
  it("should respond with 401", async () => {
    await supertest(app).get(`/api/v1/servers/${serverId}/invites`).expect(401);
  });
});