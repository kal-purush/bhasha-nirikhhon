import request from 'supertest';
import app from '../config/app';
import assert from 'assert';

describe("Budget Route", () => {
  it("Should return 400 if missing productsId", async () => {
    await request(app)
      .post('/api/budget/')
      .send({ userId: 1 })
      .expect(400)
  })

  it("Should return 400 if missing productsId", async () => {
    await request(app)
      .post('/api/budget/')
      .send({ productsId: [1] })
      .expect(400)
  })

  it("Should return 404 if user notfound", async () => {
    await request(app)
      .post('/api/budget/')
      .send({ userId: 101, productsId: [1, 2] })
      .expect(404)
  })

  it("Should return 404 if product notfound", async () => {
    await request(app)
      .post('/api/budget/')
      .send({ userId: 100, productsId: [101, 2] })
      .expect(404)
  })

  it("Should return 201 on success", async () => {
    await request(app)
      .post('/api/budget/')
      .send({ userId: 100, productsId: [10, 2] })
      .expect(201)
  })

  it("Should return the correct value on success", async () => {
    await request(app)
      .post('/api/budget/')
      .send({ userId: 1, productsId: [1, 2] })
      .then(response => {
        assert(response.body, "7410.2")
      })
  })
})