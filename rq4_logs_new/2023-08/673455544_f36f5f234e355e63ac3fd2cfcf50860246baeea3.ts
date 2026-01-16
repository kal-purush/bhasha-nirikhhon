import { z } from "zod";

export interface ResetPasswordProps {
  token: string;
}

export const ResetPasswordSchema = z
  .object({
    password: z.string().nonempty("Campo é Obrigatório"),
    passwordConfirm: z.string().min(1, "A confirmação de senha é obrigatória"),
  })
  .refine(({ password, passwordConfirm }) => password === passwordConfirm, {
    message: "As senhas precisam corresponderem",
    path: ["passwordConfirm"],
  });

export type ResetPasswordData = z.infer<typeof ResetPasswordSchema>;