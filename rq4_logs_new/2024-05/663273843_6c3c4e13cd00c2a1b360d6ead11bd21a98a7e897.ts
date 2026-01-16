import { z } from "zod";

export const createOrganizationSchema = z.object({
  name: z.string().min(3),
  logo: z.string(),
});

export type CreateOrganizationSchema = z.infer<typeof createOrganizationSchema>;

export const updateOrganizationSchema = z.object({
  name: z.string().min(3).optional(),
  logo: z.string().optional(),
});

export type UpdateOrganizationSchema = z.infer<typeof updateOrganizationSchema>;

export type Organization = z.infer<typeof organization>;

const organization = z.object({
  id: z.string(),
  name: z.string(),
  logo: z.string(),
  createdAt: z.date(),
  updatedAt: z.date(),
});