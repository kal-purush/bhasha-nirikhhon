import { v4 as uuid } from "uuid";

// export type typePessoas = {
//   idPessoa: string;
//   name: string;
//   cpf: number;
//   saldo: number;
//   dataNascimento: number;
//   valor: number;
//   data: number;
//   descricao: string;
// };

export type typeExtrato = {
  data: string;
  valor: number;
  descricao: string;
};

export type typePessoas = {
  name: string;
  cpf: number;
  saldo: number | string;
  dataNascimento: number;
  valor: number | string;
  data: string;
  extrato: typeExtrato[];
};

export const arrayPessoas: typePessoas[] = [
  {
    name: "Ana",
    cpf: 11111111111,
    saldo: 0,
    dataNascimento: 5 / 8 / 2012,
    valor: "",
    data: Date.now().toString(),
    extrato: [
      {
        data: Date.now().toString(),
        valor: 0,
        descricao: "",
      },
    ],
  },
  {
    name: "Angelo",
    cpf: 233234234234234,
    saldo: 0,
    dataNascimento: 7 / 11 / 1893,
    valor: "",
    data: Date.now().toString(),
    extrato: [
      {
        data: Date.now().toString(),
        valor: 0,
        descricao: "",
      },
    ]  
  }
]