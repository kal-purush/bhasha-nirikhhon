import { UseFormReturn } from 'react-hook-form'
import * as z from 'zod'

export const registerFormSchema = z.object({
  restaurantName: z
    .string()
    .min(2, 'O nome do estabelecimento deve ter pelo menos 2 caracteres'),
  managerName: z
    .string()
    .min(2, 'O nome do administrador deve ter pelo menos 2 caracteres'),
  phone: z.string().length(15, 'Número de telefone inválido'),
})
export type RegisterFormSchema = z.infer<typeof registerFormSchema>
export type RegisterForm = UseFormReturn<RegisterFormSchema, any, undefined>