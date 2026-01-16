import { z } from 'zod'
import { addMonths, isLastDayOfMonth } from 'date-fns'
export const createFinancialYearSchema = z
  .object({
    startdate: z.coerce.date(), // Converts input to a Date object
    enddate: z.coerce.date(), // Converts input to a Date object
    yearname: z.string().min(1, 'Year name is required'), // Must not be empty
    isactive: z.boolean().default(true), // Optional, defaults can be handled elsewhere
    createdby: z.number().int().positive(), // Must be a positive integer
  })
  .refine(
    (data) => {
      // Ensure start date is before end date
      if (data.startdate >= data.enddate) {
        return false
      }
      if (data.startdate.getDate() !== 1) {
        return false
      }

      // Check if end date is the last day of a month
      if (!isLastDayOfMonth(data.enddate)) {
        return false
      }

      return true
    },
    (data) => {
      if (data.startdate >= data.enddate) {
        return {
          message: 'The financial year must start before it ends',
          path: ['startdate', 'enddate'],
        }
      }
      if (data.startdate.getDate() !== 1) {
        return {
          message: 'The start date must be the first day of a month',
          path: ['startdate'],
        }
      }
      if (!isLastDayOfMonth(data.enddate)) {
        return {
          message: 'The end date must be the last day of a month',
          path: ['enddate'],
        }
      }
      return {
        message: 'The financial year must span exactly 12 months',
        path: ['startdate'],
      }
    }
  )

export type financialYear = z.infer<typeof createFinancialYearSchema>