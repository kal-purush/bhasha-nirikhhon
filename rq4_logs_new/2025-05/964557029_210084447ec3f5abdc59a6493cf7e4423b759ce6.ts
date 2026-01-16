import { Discount } from '@/types/cashier'

/**
 * Check if a discount is valid and can be used
 * @param discount The discount to check
 * @returns boolean indicating if the discount is valid and can be used
 */
export const isDiscountValid = (discount: Discount): boolean => {
  // Check if discount is active
  if (!discount.isActive) return false

  // Check if discount has reached max usage
  if (discount.maxUses !== null && discount.maxUses !== undefined) {
    if (discount.usedCount !== undefined && discount.usedCount >= discount.maxUses) {
      return false
    }
  }

  // Check if discount is within valid date range
  const currentDate = new Date()
  const startDate = new Date(discount.startDate)
  const endDate = new Date(discount.endDate)

  if (currentDate < startDate || currentDate > endDate) {
    return false
  }

  return true
}