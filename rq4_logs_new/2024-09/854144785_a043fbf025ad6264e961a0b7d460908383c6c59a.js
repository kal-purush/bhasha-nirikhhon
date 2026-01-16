// ATIVIDADE 01
var indice = 13, soma = 0, k =0;
while(k<indice){
    k += 1;
    soma += k;
}
console.log(soma);
//RESPOSTA = 91


// ATIVIDADE 02
//PRATICAMENTE O MESMO CÓDIGO EM OUTRAS LINGUAGENS COMO C e JAVA
function verificarFibonacci(numero){
    var sequenciaAtual = 0;
    var numeroAnterior2 = 0;
    var numeroAnterior = 1;
    while(numero > sequenciaAtual && numero !== sequenciaAtual)
    {
        numeroAnterior2 = numeroAnterior;
        numeroAnterior = sequenciaAtual;
        sequenciaAtual = numeroAnterior + numeroAnterior2;
    }
    if(numero === sequenciaAtual)
    {
        console.log("Pertence a sequencia.");
    }
    else
    {
        console.log("Nao pertence");
    }
}

// ATIVIDADE 03
  function processarFaturamento() {

    const faturamentoJson = `{
        "faturamento": [
            { "dia": 1, "valor": 1000 },
            { "dia": 2, "valor": 0 },
            { "dia": 3, "valor": 1500 },
            { "dia": 4, "valor": 1200 },
            { "dia": 5, "valor": 0 },
            { "dia": 6, "valor": 800 },
            { "dia": 7, "valor": 0 }
        ]
    }`;
    
    const dados = JSON.parse(faturamentoJson).faturamento;
    
    let menorValor = Infinity;
    let maiorValor = -Infinity;
    let somaValores = 0;
    let diasComFaturamento = 0;
    let diasAcimaMedia = 0;
    
    for (const item of dados) {
        const valor = item.valor;
        if (valor > 0) {
            if (valor < menorValor) menorValor = valor;
            if (valor > maiorValor) maiorValor = valor;
            somaValores += valor;
            diasComFaturamento++;
        }
    }
    
    const media = diasComFaturamento > 0 ? somaValores / diasComFaturamento : 0;
    
    for (const item of dados) {
        if (item.valor > media) {
            diasAcimaMedia++;
        }
    }
    
    console.log("Menor valor de faturamento:", menorValor);
    console.log("Maior valor de faturamento:", maiorValor);
    console.log("Número de dias com faturamento superior à média mensal:", diasAcimaMedia);
}

// ATIVIDADE 04
//TAMBÉM POSSÍVEL FAZER COM OBJETOS PARA DEIXA-LO MAIS DINÂMICO
function calcularPorcentagem(){
    const SP = 67836.43;
    const RJ = 36678.66;
    const MG = 29229.88;
    const ES = 27165.48;
    const OUTROS = 19849.53;

    let total = SP + RJ + MG + ES + OUTROS;

    let spPorcent =  SP *100 / total;
    let rjPorcent =  RJ *100 / total;
    let mgPorcent =  MG *100 / total;
    let esPorcent =  ES *100 / total;
    let outrosPorcent =  OUTROS *100 / total;
    console.log("A porcentagem de Sao Paulo: " + spPorcent.toFixed(2));
    console.log("A porcentagem do Rio de Janeiro: " + rjPorcent.toFixed(2));
    console.log("A porcentagem de Minas Gerais: " + mgPorcent.toFixed(2));
    console.log("A porcentagem de Espirito Santo: " + esPorcent.toFixed(2));
    console.log("A porcentagem de Outros: " + outrosPorcent.toFixed(2));
}

//ATIVIDADE 05
//CASO FOSSE FEITA EM C, EU UTILIZARIA UMA PILHA COM PONTEIROS!
function inverterString(str) {
    let resultado = '';
    for (let i = str.length - 1; i >= 0; i--) {
        resultado += str[i];
    }
    console.log('String original:', str);
    console.log('String invertida:', resultado);
}