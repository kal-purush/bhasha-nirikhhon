import {z} from "zod";

const phoneNumberRegExp = /^(\+\s?\d{1,3}\s?)?(\(\d{1,4}\)\s?)?[0-9\s]{9,}$/;

export const consultationFormSchema = z.object({
  firstname: z.string().min(1, { message: "Please enter your first name" }),
  lastname:  z.string().min(1, { message: "Please enter your last name" }),
  email: z.string().email({ message: "Please enter your email address in the format: text@example.com" }),
  phoneNumber: z.string().regex(phoneNumberRegExp, { message: 'Phone number entered is not valid' }),
  country: z.string().min(1, { message: "Please enter your country" }),
  comapny: z.string().min(1, { message: "Please enter your company's name" }),
})