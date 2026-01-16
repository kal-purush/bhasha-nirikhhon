import { z } from 'zod';

export const updateTenantClientSchema = z.object({
  company: z.string().trim().min(1, { message: 'Company name is required' }),
  email: z
    .string()
    .trim()
    .min(1, { message: 'Company email is required' })
    .email({ message: 'Invalid email' }),
});