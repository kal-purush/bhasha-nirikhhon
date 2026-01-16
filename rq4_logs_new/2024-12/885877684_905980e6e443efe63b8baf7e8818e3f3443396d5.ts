import { fetchApi } from '@/utils/http'
import { z } from 'zod'

export const SignInRequestSchema = z.object({
  username: z.string().min(1),
  password: z.string().min(1),
})

export const SignInResponseSchema = z.object({
  token: z.string(),
  user: z.object({
    id: z.string(),
    email: z.string(),
    name: z.string().optional(),
  }),
})

export type SignInRequest = z.infer<typeof SignInRequestSchema>
export type SignInResponse = z.infer<typeof SignInResponseSchema>

export async function signIn(credentials: SignInRequest) {
  return fetchApi<SignInResponse>({
    url: '/api/auth/login',
    method: 'POST',
    body: credentials,
    schema: SignInResponseSchema,
  })
}