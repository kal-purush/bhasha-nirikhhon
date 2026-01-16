import { t } from "i18next"
import { z } from "zod"
import { castNumber } from "../../../lib/cast-number"

export const ConditionalPriceSchema = z
  .object({
    amount: z.union([z.string(), z.number()]),
    gte: z.union([z.string(), z.number()]).nullish(),
    lte: z.union([z.string(), z.number()]).nullish(),
    lt: z.number().nullish(),
    gt: z.number().nullish(),
    eq: z.number().nullish(),
  })
  .refine((data) => data.amount !== "", {
    message: t(
      "stockLocations.shippingOptions.conditionalPrices.errors.amountRequired"
    ),
    path: ["amount"],
  })
  .refine(
    (data) => {
      const hasEqLtGt =
        data.eq !== undefined || data.lt !== undefined || data.gt !== undefined

      // The rule has operators that can only be managed using the API, so we should not validate this.
      if (hasEqLtGt) {
        return true
      }

      return (
        (data.gte !== undefined && data.gte !== "") ||
        (data.lte !== undefined && data.lte !== "")
      )
    },
    {
      message: t(
        "stockLocations.shippingOptions.conditionalPrices.errors.minOrMaxRequired"
      ),
      path: ["gte"],
    }
  )
  .refine(
    (data) => {
      if (
        data.gte != null &&
        data.gte !== "" &&
        data.lte != null &&
        data.lte !== ""
      ) {
        const gte = castNumber(data.gte)
        const lte = castNumber(data.lte)
        return gte <= lte
      }
      return true
    },
    {
      message: t(
        "stockLocations.shippingOptions.conditionalPrices.errors.minGreaterThanMax"
      ),
      path: ["gte"],
    }
  )

export type ConditionalPrice = z.infer<typeof ConditionalPriceSchema>

export const UpdateConditionalPriceSchema = ConditionalPriceSchema.and(
  z.object({
    id: z.string().optional(),
  })
)

export type UpdateConditionalPrice = z.infer<
  typeof UpdateConditionalPriceSchema
>

export const CondtionalPriceRuleSchema = z.object({
  prices: z.array(ConditionalPriceSchema),
})

export type CondtionalPriceRuleSchemaType = z.infer<
  typeof CondtionalPriceRuleSchema
>

export const UpdateConditionalPriceRuleSchema = z.object({
  prices: z.array(UpdateConditionalPriceSchema),
})

export type UpdateConditionalPriceRuleSchemaType = z.infer<
  typeof UpdateConditionalPriceRuleSchema
>