import { z } from "zod"

const schema = z
  .object({
    username: z
      .string()
      .min(4, {
        message: "Korisnicko ime mora biti dugacko najmanje 4 karaktera",
      })
      .regex(new RegExp("^\\S{4,}$"), {
        message: "Razmak nije dozvoljen u korisnickom imenu",
      }),
    email: z.string().email({ message: "Email nije validan" }),
    password: z
      .string()
      .min(4, { message: "Sifra mora biti dugacka najmanje 4 karaktera" })
      .regex(new RegExp("(?=.*[a-z])(?=.*[A-Z])(?=.*[0-9])(?=.{4,}).*$"), {
        message: "Sifra mora sadrzati veliko slovo, malo slovo i broj",
      }),
    confirmPassword: z.string(),
  })
  .refine((schema) => schema.confirmPassword === schema.password, {
    message: "Sifra i potvrdjena sifra se ne podudaraju",
  });

  export type FormData = z.infer<typeof schema>;

  export default schema;