let pesoPeca = 101;

if (pesoPeca > 100) {
    console.log("A Peça foi cadastrada!");
} else {
    console.log("Ocorreu um Erro! A peça pesa menos de 100g")
};

let numeroPecas = 12;

if (numeroPecas < 10) {
    console.log("As Peças foram cadastradas!");
} else {
    console.log("O limite de peças foi excedido!")
};

let nomePeca = "Amortecedor";

console.log ("O comprimento do nome da peça é " + nomePeca.length + " letras");

let tamanhoNome = nomePeca.length;

if (tamanhoNome < 3 ) {
    console.log("Ocorreu um Erro! Não é possível cadastrar pois o nome é curto!");
} else {
    console.log("A Peça foi cadastrada!")
}