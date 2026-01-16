import { z } from 'zod'

export const testimonialScheme = z.object({
  name: z.string().nonempty('Поле має бути заповнене').max(25, 'Ім’я має містити максимум 25 символів'),

  email: z
    .string()
    .nonempty('Поле має бути заповнене')
    .refine(
      (value) =>
        /^[a-zA-Z0-9_%+-]*(?:\.[a-zA-Z0-9_%+-]+)?[a-zA-Z0-9_%+-]+@[a-zA-Z0-9_%+-]*(?:\.[a-zA-Z0-9_%+-]+)?[a-zA-Z0-9_%+-]+\.[a-zA-Z]{2,}$/.test(
          value
        ),
      {
        message: 'Некоректно введений email'
      }
    ),

  message: z
    .string()
    .nonempty('Введіть текст відгуку')
    .min(2, 'Відгук має містити мінімум 2 символи')
    .max(150, 'Відгук має містити максимум 150 символів')
})