import { Pessoa } from "types/ModelsAPI";
import { Person, Phone, UserAddress } from "types/Plan";

export default function mapPessoaToLocalPerson(pessoa: Pessoa): Person {
  const phones: Phone[] = pessoa.telefones
    ? pessoa.telefones.map((telefone) => ({
        phone: telefone.ddd_numero,
        obs: telefone.observacao,
        priority: telefone.prioridade ? 1 : 0,
        type: telefone.tipo === "celular" ? "celular" : "fixo",
      }))
    : [];

  const addresses: UserAddress[] = pessoa.enderecos
    ? pessoa.enderecos.map((endereco) => ({
        cep: endereco.cep || "",
        city: endereco.localidade || "",
        complement: endereco.complemento || "",
        id: endereco.id || "",
        neighbor: endereco.bairro,
        number: endereco.numero || "",
        state: endereco.uf,
        street: endereco.logradouro,
        identification: endereco.identificacao,
        latitude: endereco.latitude,
        longitude: endereco.longitude,
      }))
    : [];

  const person: Person = {
    id: pessoa.id || "",
    name: pessoa.nome || "",
    surname: pessoa.sobrenome || "",
    birthDate: pessoa.nascimento || "",
    emails: pessoa.emails ? pessoa.emails : [],
    gender: (pessoa.sexo as any) || "male",
    role: pessoa.trabalhoFuncao || "",
    status: 0,
    phones,
    addresses,
    documents: [],
  };

  return person;
}