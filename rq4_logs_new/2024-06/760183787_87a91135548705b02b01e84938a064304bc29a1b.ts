import z from "zod";

export const productsMainFormSchema = z.object({
  code: z
    .number({ required_error: "Código é obrigatório" })
    .int("Código deve ser um número inteiro")
    .gt(0, "Código deve ser maior que zero"),
  name: z.string().min(1, "Nome é obrigatório"),
  description: z.string().optional(),
  costPrice: z
    .number()
    .gt(0, "Preço de Custo deve ser maior que zero")
    .optional(),
  profitMargin: z
    .number()
    .gt(0, "Margem de Lucro deve ser maior que zero")
    .optional(),
  sellingPrice: z
    .number({ required_error: "Preço de Venda é obrigatório" })
    .gt(0, "Preço de Venda deve ser maior que zero"),
  stock: z
    .number({ required_error: "Estoque é obrigatório" })
    .gt(0, "Estoque deve ser maior que zero"),
  unitOfMeasurement: z.object(
    {
      externalId: z.string({ required_error: "ID é obrigatório" }),
      name: z.string({ required_error: "Nome é obrigatório" }),
    },
    { invalid_type_error: "Unidade é obrigatório" }
  ),
  isActive: z.coerce.boolean({ required_error: "Status é obrigatório" }),
});

export type ProductsMainFormData = z.infer<typeof productsMainFormSchema>;