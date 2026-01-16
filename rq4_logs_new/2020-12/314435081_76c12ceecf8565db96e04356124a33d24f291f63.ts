import { Pessoa } from "types/ModelsAPI";
import { Person } from "types/Plan";
import mapAddressToEndereco from "../address/mapAddressToEndereco";

export default function mapPersonToPessoa(person: Person): Pessoa {
  const pessoa: Pessoa = {
    nome: person.name,
    sobrenome: person.surname,
    emails: person.emails,
    nascimento: person.birthDate,
    sexo: person.gender,
    enderecos: person.addresses.map((address) => mapAddressToEndereco(address)),
    telefones: person.phones.map((phone) => ({
      ddd_numero: phone.phone,
      observacao: phone.obs,
      prioridade: phone.priority,
      tipo: phone.type,
    })),
    trabalhoFuncao: person.role || "",
  };

  return pessoa;
}