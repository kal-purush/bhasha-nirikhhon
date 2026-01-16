import { z } from "zod";

export const phoneSchema = z.object({
  id: z.string(),
  phone: z.string(),
});

export const addPhoneSchema = z.object({
  phone: z.string(),
});

export const emailSchema = z.object({
  id: z.string(),
  email: z.string(),
});

export const addEmailSchema = z.object({
  email: z.string(),
});

export type Phone = z.infer<typeof phoneSchema>;
export type Email = z.infer<typeof emailSchema>;