import { Ok, asRequestListener } from "funkster-http";
import { sendJson } from '../src';
import * as http from "http";
import * as request from "supertest";

describe("When sending a json response", () => {
  const jsonObj = { success: true };
  const pipe = sendJson(jsonObj);
  const server = http.createServer(asRequestListener(pipe));

  it("should return the correct json response", () =>
    request(server)
      .get("/")
      .expect("Content-Type", /json/)
      .expect(200, JSON.stringify(jsonObj)));
});