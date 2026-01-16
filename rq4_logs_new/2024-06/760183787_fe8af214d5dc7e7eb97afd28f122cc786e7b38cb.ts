import z from "zod";

import { cellphoneRegex } from "../../../utils/masks";

export const customersMainFormSchema = z.object({
  code: z
    .number({ required_error: "Código é obrigatório" })
    .int("Código deve ser um número inteiro")
    .gt(0, "Código deve ser maior que zero"),
  type: z.object(
    {
      key: z.number({ required_error: "Key é obrigatório" }),
      label: z.string({ required_error: "Label é obrigatório" }),
    },
    { invalid_type_error: "Tipo de Pessoa é obrigatório" }
  ),
  document: z.string().min(1, "Documento é obrigatório"),
  fullName: z.string().min(1, "Nome é obrigatório"),
  cellphone: z
    .string()
    .min(1, "Celular é obrigatório")
    .regex(cellphoneRegex, "Celular é inválido"),
  email: z.string().min(1, "Email é obrigatório").email("Email inválido"),
  isActive: z.coerce.boolean({ required_error: "Status é obrigatório" }),
});

export type CustomersMainFormData = z.infer<typeof customersMainFormSchema>;