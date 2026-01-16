// Comentário em linha

/*
    Comentário
        em
      Bloco
*/

//Permite escrever uma mensagem no terminal
console.log('Testando o node.JS');

console.log('printar no terminar');

// Import da biblioteca que faz entrada de dados pelo usário
var readline = require ('readline');

// Criamos um objeto entradaDados com as referencias de readline
var entradaDados = readline.createInterface({
    input: process.stdin, 
    output: process.stdout
});

/*
        O que é uma função de de call back -
            é uma função que quando chamada ela retorna o seu conteudo em uma única
            instrução, ou seja, em apenas um passo na programão
*/



// Criamos uma função de call back para receber os dados digitados pelo usuário
    /*
        (O objeto entradaDados aguarda a digitção do usuário, após a digitação do conteúdo é
          acionada um call back para encaminhar os dados para o console.log. Esse conteúdo foi
          encaminhado através do parametro dados da função)
    */
entradaDados.question("Digite seu nome: \n", function (dados){
    console.log('Bem Vindo, ' + dados + ' ao servidor node.JS')

});