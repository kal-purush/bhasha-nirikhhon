// Ecercício 1 
// a)
// const minhaString: string = 3 
// R - Tipo number não pode ser atribuido ao tipo string

// b)
const meuNumero: number = 3
const minhaStringMeuNumero: {string: string, numero:number} = {
    string: "qualquer coisa",
    numero: 33
}
//  ou 
let nome: string | number =''
nome = "qualquer coisa"
nome = 33

// c)
const novoObjeto: {nome: string, idade: number, corFavorita: string} = {
    nome: "Wictor",
    idade: 33,
    corFavorita: "azul"
}

// d)
enum cores {
    VERMELHO= "vermelho",
    LARANJA= "laranja",
    AMARELO= "amarelo",
    VERDE= "verde",
    AZUL="azul",
    AZUL_ESCURO="azul escuro",
    VIOLETA="violeta"
}

type Pessoa = {
    nome: string,
    idade: number,
    corFavorita: cores
}