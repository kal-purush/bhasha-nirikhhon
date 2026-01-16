import Elysia from 'elysia'
import { z } from 'zod'
import { createId } from '@paralleldrive/cuid2'
import * as argon2 from 'argon2'

const registerCustomerBodySchema = z.object({
  name: z.string().min(1),
  password: z.string().min(8),
  phone: z.string(),
  email: z.string().email(),
})

export const registerCustomer = new Elysia().post(
  '/customers',
  async ({ body, set }) => {
    const { name, email, phone, password } =
      registerCustomerBodySchema.parse(body)

    set.status = 201

    const id = createId()
    const role = 'customer'
    const createdAt = new Date()
    const updatedAt = new Date()
    const passwordHash = await argon2.hash(password)

    return {
      id,
      name,
      email,
      phone,
      passwordHash,
      role,
      createdAt,
      updatedAt,
    }
  }
)