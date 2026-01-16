import request from "supertest"
import { test, expect, describe } from "vitest"
import { app } from "../../../../tests/setup-fastify-e2e"

describe('register student (E2E)', () => {
  test('[POST] /students', async () => {
    const user = await request(app.server).post("/instructors").send({
      document: "11111111112",
      email: "instructor@email.com",
      first_name: "John",
      last_name: "Doe",
      password: "Teste123",
      phone_number: "11999999999"
    })

    const authentication = await request(app.server).post("/sessions").send({
      email: "instructor@email.com",
      password: "Teste123",
    })

    const response = await request(app.server).post("/courses").send({
      course_name: "course 1",
      instructor_id: "instructor-1",
      description: "description"
    })

    expect(response.status).toBe(201)
  })
})