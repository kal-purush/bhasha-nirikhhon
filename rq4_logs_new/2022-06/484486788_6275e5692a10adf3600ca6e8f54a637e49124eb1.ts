import request from "supertest";

import { app } from "../../app";

it("completes the book when the request is all good", async () => {
  const signInCookie = global.getSignInCookie();

  await request(app).post("/api/books").set("Cookie", signInCookie).send({
    title: "1",
    body: "1",
    author: "1",
  });

  await request(app).post("/api/books").set("Cookie", signInCookie).send({
    title: "2",
    body: "2",
    author: "2",
  });

  const allBooksReq = await request(app)
    .get("/api/books")
    .set("Cookie", signInCookie)
    .send({})
    .expect(200);

  const book1Id = allBooksReq.body[0].id;
  const book2Id = allBooksReq.body[1].id;

  await request(app)
    .patch(`/api/books/${book1Id}`)
    .set("Cookie", signInCookie)
    .send({})
    .expect(200);

  const book1Req = await request(app)
    .get(`/api/books/${book1Id}`)
    .set("Cookie", signInCookie)
    .send({})
    .expect(200);

  expect(book1Req.body!.status).toBe("completed");

  const book2Req = await request(app)
    .get(`/api/books/${book2Id}`)
    .set("Cookie", signInCookie)
    .send({})
    .expect(200);

  expect(book2Req.body!.status).toBe("created");
});