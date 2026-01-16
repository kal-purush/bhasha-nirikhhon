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

describe("Create room", () => {
  const roomInfo = {
    name: "Foo",
  };

  // Create
  it("should respond with 201", async () => {
    const { body } = await supertest(app)
      .post(`/api/v1/servers/${serverId}/rooms`)
      .send(roomInfo)
      .set("Authorization", `Bearer ${accessToken}`)
      .expect(201);
    expect(body.room.name).toBe(roomInfo.name);
  });

  // Existing room name
  it("should respond with 409", async () => {
    await supertest(app)
      .post(`/api/v1/servers/${serverId}/rooms`)
      .send(roomInfo)
      .set("Authorization", `Bearer ${accessToken}`)
      .expect(409);
  });

  // Unauthorized
  it("should respond with 401", async () => {
    await supertest(app)
      .post(`/api/v1/servers/${serverId}/rooms`)
      .send(roomInfo)
      .expect(401);
  });

  // Invalid payload
  it("should respond with 400", async () => {
    await supertest(app)
      .post(`/api/v1/servers/${serverId}/rooms`)
      .send({
        invalidField: "this is an invalid field",
      })
      .set("Authorization", `Bearer ${accessToken}`)
      .expect(400);
  });
});