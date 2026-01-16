//Função para inserir os simbolos/números na telinha.
function insert(num){

     // objeto documento e metodo get.

     // Crição da variavel "numero" para sempre adicionar um digito novo sem apagar o antigo.

    let numero = document.getElementById('resultado').innerHTML;
    document.getElementById('resultado').innerHTML = numero + num;
}

//Função para limpar a telinha.
function clean(){

    // objeto documento e metodo get.
    document.getElementById('resultado').innerHTML = "";
}

//Função para apagar 1 digito.
function back(){

    // Macete para fazer um botão "apagar".

    // objeto documento e metodo get.

    // Crição da variavel "resultado" para sempre agir no que está digitado na telinha.

    let resultado = document.getElementById('resultado').innerHTML;
    document.getElementById('resultado').innerHTML = resultado.substring(0, resultado.length -1);
    // resultado.substring(numeroStart/inicial, variavel.length quantosDiminuir);
}
function calcular(){

    // objeto documento e metodo get.

    // Crição da variavel "resultado" para sempre agir no que está digitado na telinha.

    let resultado = document.getElementById('resultado').innerHTML;

    //if para caso tenha algo digitado na telinha.
    if(resultado){
        document.getElementById('resultado').innerHTML = eval(resultado);
        //eval() --> função global que avaliar uma expressão ou declaração em uma string.

    }
    //else para caso não tenha nada digitado na telinha.
    else{
        document.getElementById('resultado').innerHTML = "Inválido"
    }
}