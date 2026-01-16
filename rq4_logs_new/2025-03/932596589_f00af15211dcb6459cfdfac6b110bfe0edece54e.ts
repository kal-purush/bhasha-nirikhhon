import { requiredMessage } from "@/lib/constant/errorMessage"
import { z } from "zod"

export const companyInfoSchema = z.object({
  companyName: z.string().min(1, requiredMessage("Company name ")),
  payrollStatus: z
    .string()
    .min(1, requiredMessage("Payroll status is required ")),
  email: z.string().email("Invalid email"),
  payrollStatusOthers: z
    .string()
    .optional()
    .refinement((val, ctx) => {
      const parent = ctx?.parent as { payrollStatus: string }
      // If payrollStatus is "others", payrollStatusOthers must be non-empty
      return (
        parent?.payrollStatus !== "others" || (val && val.trim().length > 0)
      )
    }, "Please specify payroll status if 'Others' is selected"),
  contactNumber: z.string().min(1, requiredMessage("Contact number ")),
  contactPerson: z.string().min(1, requiredMessage("Contact person ")),
  estimatedNumberOfEmployee: z
    .number()
    .min(1, "The number of employee should less than 1"),
})