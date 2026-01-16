import ParamsI from "../../__interfaces/queryParams.interface";
import { createResponseSchema } from "../../__interfaces/response.interface";
import { z } from "zod";

export const cashRegisterSchema = z.object({
  caixa_id: z.string().optional().nullable(),
  caixa_user_name: z.string().optional().nullable(),
  caixa_user_role: z.string().optional().nullable(),
  closing_date: z.string().optional().nullable(),
  opening_date: z.string().optional().nullable(),
  opening_user_name: z.string().optional().nullable(),
  opening_user_role: z.string().optional().nullable(),
  sector: z.string().optional().nullable(),
  sub_sector: z.string().optional().nullable(),
  terminal_serial: z.string().optional().nullable(),
});

export const cashRegisterResponseSchema =
  createResponseSchema(cashRegisterSchema);

export type cashRegisterType = z.infer<typeof cashRegisterSchema>;

export interface cashRegisterParams extends ParamsI {
  s?: string;
  f?: string;
  event_id?: string;
}