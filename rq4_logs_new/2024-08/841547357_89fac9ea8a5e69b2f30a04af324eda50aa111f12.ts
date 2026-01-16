import { z } from "zod";
import { cnpjRegex, passwordRegex } from "../../utils/regex";
import { isValidCNPJ } from "../../utils/ValidFunctions";

export type restaurantData = z.infer<typeof schema>


export const schema = z.object({
    email: z.string().email('Digite um email valido!'),
    cnpj: z.string().min(14, "O campo CNPJ é obrigatório").regex(cnpjRegex, 'Insira um CNPJ válido').refine(isValidCNPJ, 'insira um CNPJ válido'),
    password: z.string().regex(passwordRegex, 'A senha deve conter no mínimo 8 digitos, 1 caractere especial e 1 número.').min(8, 'Senha é obrigatória!'),
    confirmPassword: z.string().regex(passwordRegex, "Confirme sua senha, elas devem ser iguais!").min(8, "Confirme sua senha, elas devem ser iguais!"),
});