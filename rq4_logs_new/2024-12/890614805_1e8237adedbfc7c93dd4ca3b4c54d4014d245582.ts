import { z } from "zod";

/**
 * Schema for sign in form
 * @constant
 * @type {z.ZodObject}
 * @see
 */
const signInSchema = z.object({
  username: z
    .string()
    .min(3, { message: "Brugernavnet skal være mindst 3 tegn langt" })
    .max(30, { message: "Brugernavnet må ikke være længere end 30 tegn" }),
  password: z
    .string()
    .min(6, { message: "Adgangskoden skal være mindst 6 tegn lang" }),
});

/**
 * Schema for sign up form
 * @constant
 * @type {z.ZodObject}
 * @see
 */
const signUpSchema = z
  .object({
    name: z
      .string()
      .min(3, { message: "Navnet skal være mindst 3 tegn langt" })
      .max(30, { message: "Navnet må ikke være længere end 30 tegn" }),
    username: z
      .string()
      .min(3, { message: "Brugernavnet skal være mindst 3 tegn langt" })
      .max(30, { message: "Brugernavnet må ikke være længere end 30 tegn" }),
    email: z.string().email({ message: "Indtast en gyldig emailadresse" }),
    password: z
      .string()
      .min(6, { message: "Adgangskoden skal være mindst 6 tegn lang" }),
    repeatPassword: z.string(),
  })
  .refine((data) => data.password === data.repeatPassword, {
    message: "Adgangskoderne skal være ens",
    path: ["repeatPassword"],
  });

export { signInSchema, signUpSchema };