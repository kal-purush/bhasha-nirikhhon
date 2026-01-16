import request from "supertest"
import { test, expect, describe } from "vitest"
import { app } from "../../../../tests/setup-fastify-e2e"

describe('authenticate user (E2E)', () => {
  test('[POST] /auth', async () => {
    const response = await request(app.server).post("/auth").send({
      email: "instructor@email.com",
      password: "Teste123",
    }) 

    expect(response.status).toBe(200)
  })

  test('[POST] /auth with invalid email', async () => {
    const response = await request(app.server).post("/auth").send({
      email: "instructo2@email.com",
      password: "Teste123",
    }) 

    expect(response.status).toBe(400)
  })

  test('[POST] /auth with invalid password', async () => {
    const response = await request(app.server).post("/auth").send({
      email: "instructor@email.com",
      password: "Teste1234",
    }) 

    expect(response.status).toBe(400)
  })
})