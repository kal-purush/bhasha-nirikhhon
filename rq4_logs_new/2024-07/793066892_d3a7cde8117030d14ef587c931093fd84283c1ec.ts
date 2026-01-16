import { z } from "zod";

export const AccountPersonalInfoSchema = z.object({
  firstName: z.string({ required_error: "Please enter your first name!" }),
  lastName: z
    .string({ required_error: "Please enter your first name!" })
    .optional(),
  phone: z
    .string({ required_error: "Please enter your phone number" })
    .max(10)
    .optional(),
  email: z
    .string({ required_error: "Please enter your email" })
    .email({ message: "Invalid email address" }),
  gender: z.string({ required_error: "Please select your gender" }),
  dateOfBirth: z
    .object({
      from: z.date(),
      to: z.date(),
    })
    .optional(),
  country: z.string().optional(),
  address: z.string().optional(),
  pinCode: z.string().optional(),
  schoolOrCollegeName: z.string().optional(),
  schoolOrCollegeAddress: z.string().optional(),
});