import { z } from 'zod'

const passwordValidation = new RegExp(
  /^(?=.*?[A-Z])(?=.*?[0-9])(?=.*?[#?!@$%^&*-]).{6,}$/
)

export const ChangePasswordValidation = z.object({
  password: z
    .string()
    .min(6, { message: 'Пароль повинен містити не менше 6 символів.' })
    .max(12, { message: 'Пароль повинен містити не більше 12 символів' })
    .regex(passwordValidation, {
      message: 'Пароль повинен містити щонайменше одну велику літеру, одну цифру та спеціальний символ',
    }),
  confirmPassword: z
    .string()
    .min(6, { message: 'Пароль повинен містити не менше 6 символів.' })
    .max(12, { message: 'Пароль повинен містити не більше 12 символів' })
    .regex(passwordValidation, {
      message: 'Your password is not valid',
    })
}).refine((data) => data.password === data.confirmPassword, {
  path: ['confirmPassword'],
  message: 'Паролі не співпадають'
})

export type FormFields = z.infer<typeof ChangePasswordValidation>