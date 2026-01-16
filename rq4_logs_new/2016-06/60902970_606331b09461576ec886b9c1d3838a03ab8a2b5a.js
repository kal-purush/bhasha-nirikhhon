import * as supertest from 'supertest-as-promised';
import should from "should";
import app from "../../app.js";

const request = supertest.agent(app.listen());

describe("test api /topActiveUsers", () => {

  it("tests page 0 has data", async () => {
    const { body: result } = await request.get(`/topActiveUsers?page=0`);
    //console.log(result);
    should.exist(result);
    should.exist(result[0].id);
  });

});

describe("test api /users", () => {

  it("tests with user id 1", async () => {
    const { body: result } = await request.get(`/users?id=1`);
    //console.log(result);
    should.exist(result);
    should.exist(result.id);
  });

});