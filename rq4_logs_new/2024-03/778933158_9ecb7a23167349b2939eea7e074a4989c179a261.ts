import { z } from 'zod'

export const AVAILABLE_SIZES = ['S', 'M', 'L'] as const
export const AVAILABLE_COLORS = [
  'white',
  'beige',
  'green',
  'purple',
  'blue',
] as const
export const AVAILABLE_SORT = ['none', 'price-asc', 'price-desc'] as const

export const ProductFilterValidator = z.object({
  sort: z.enum(AVAILABLE_SORT),
  color: z.array(z.enum(AVAILABLE_COLORS)),
  size: z.array(z.enum(AVAILABLE_SIZES)),
  price: z.tuple([z.number(), z.number()]),
})

export type ProductState = Omit<
  z.infer<typeof ProductFilterValidator>,
  'price'
> & {
  price: { isCustom: boolean; range: [number, number] }
}