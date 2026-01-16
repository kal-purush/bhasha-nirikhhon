import { z } from "zod";
import { cepRegex, numberRegex } from "../../utils/regex";


export type RestaurantAdressData = z.infer<typeof schema>


export const schema = z.object({
    nameAdress: z.string().min(1, 'Digite um nome para o endereço!'),
    numberRestaurant: z.string().min(1, "Digite o número correspondente").regex(numberRegex, 'Digite um número'),
    cep: z.string().min(1, "Digite o cep para continuar").regex(cepRegex, 'Digite um cep para continuar'),
    road: z.string().min(1, "Digite o nome da rua"),
    city: z.string().min(1, "Digite o nome da cidade"),
    neighborhood: z.string().min(1, "Digite o nome do bairro"),
    state: z.string().min(1, "Digite o estado"),
});