import z from 'zod'

const MAX_MAIL_SIZE = 100;
const MAX_PASSWORD_SIZE = 20;
const MIN_PASSWORD_SIZE = 8;

const lowercaseRegex = /[a-z]/;              
const uppercaseRegex = /[A-Z]/;              
const numberRegex = /[0-9]/;                 
const symbolRegex = /[!@#$%^&*()_+={}\[\]|\\:;"'<>,.?/-]/;

export const emailSchema = z.object({
    email: z
        .string({ required_error: "Email is required" })
        .trim()
        .max(MAX_MAIL_SIZE, `Email should be not exceed ${MIN_PASSWORD_SIZE} characters`)
        .email("Please enter a valid email address")
})

export const nameSchema = z.object({
    name: z.string({ required_error: "Name is required"})
})

export const passwordSchema = z.object({
  password: z
    .string({ required_error: "Password is required" })
    .trim()
    .min(MIN_PASSWORD_SIZE, `Password should be at least ${MIN_PASSWORD_SIZE} characters`)
    .max(MAX_PASSWORD_SIZE, `Password should not exceed ${MAX_PASSWORD_SIZE} characters`)
});

const refinedPasswordSchema = z
  .string({ required_error: "Password is required" })
  .trim()
  .min(MIN_PASSWORD_SIZE, `Password should be at least ${MIN_PASSWORD_SIZE} characters long`)
  .max(MAX_PASSWORD_SIZE, `Password should not exceed ${MAX_PASSWORD_SIZE} characters`)
  .refine((value) => lowercaseRegex.test(value), {
    message: "Password should contain at least one lowercase letter",
  })
  .refine((value) => uppercaseRegex.test(value), {
    message: "Password should contain at least one uppercase letter",
  })
  .refine((value) => numberRegex.test(value), {
    message: "Password should contain at least one number",
  })
  .refine((value) => symbolRegex.test(value), {
    message: "Password should contain at least one symbol",
  });


export const registerSchema = z.object({
    ...nameSchema.shape,
    ...emailSchema.shape,
    password: refinedPasswordSchema
})

export const loginSchema = z.object({
    ...emailSchema.shape,
    ...passwordSchema.shape,
})