/*

type automato = {
  estados: string[];
  alfabeto: alfabeto[];
  transicoes: transicao[];
  estadoInicial: string;
  estadosFinais: string[];
};

type transicao = {
  estadoAtual: string;
  simbolo: alfabeto;
  estadoDestino: string;
};

type alfabeto = "a" | "b";

//definição formal do automato
const inputA = {
  estados: ["q0", "q1", "q2", "q3"],
  alfabeto: ["a", "b"] as alfabeto[],
  transicoes: [
    { estadoAtual: "q0", simbolo: "a" as alfabeto, estadoDestino: "q1" },
    { estadoAtual: "q0", simbolo: "b" as alfabeto, estadoDestino: "q2" },
    //   { estadoAtual: "q0", simbolo: "b" as alfabeto, estadoDestino: "q3" },
    { estadoAtual: "q1", simbolo: "a" as alfabeto, estadoDestino: "q1" },
    { estadoAtual: "q1", simbolo: "b" as alfabeto, estadoDestino: "q3" },
    { estadoAtual: "q2", simbolo: "a" as alfabeto, estadoDestino: "q3" },
    { estadoAtual: "q2", simbolo: "b" as alfabeto, estadoDestino: "q2" },
    { estadoAtual: "q3", simbolo: "a" as alfabeto, estadoDestino: "q3" },
    { estadoAtual: "q3", simbolo: "b" as alfabeto, estadoDestino: "q3" },
  ],
  estadoInicial: "q0",
  estadosFinais: ["q3", "q0"],
};
//string de entrada
const inputB = "";

function getTransicoesPossiveis(
  automatoRecebido: automato,
  simbolo: string,
  estadoAtual: string
) {
  return automatoRecebido.transicoes.filter(
    (transicao) =>
      transicao.estadoAtual === estadoAtual && transicao.simbolo === simbolo
  );
}

function getTransicoesPossiveisComSimbolo(
  automatoRecebido: automato,
  simbolo: string
) {
  return automatoRecebido.transicoes.filter(
    (value) => value.simbolo === simbolo
  );
}

//automato finito não deterministico que valida a string de entrada
function main(automatoRecebido: automato, stringDeEntrada: string): boolean {
  let estadoAtual = automatoRecebido.estadoInicial;
  for (let i = 0; i < stringDeEntrada.length; i++) {
    console.log("estado atual: ", estadoAtual);
    const simbolo = stringDeEntrada[i];
    //arrays with all transitions with the current symbol
    const transicoesPossiveis = getTransicoesPossiveis(
      automatoRecebido,
      simbolo,
      estadoAtual
    );
    console.log(transicoesPossiveis);
    if (transicoesPossiveis.length === 0)
      throw new Error(
        "Não existe transição com o símbolo: " +
          simbolo +
          " no estado: " +
          estadoAtual
      );
    estadoAtual = transicoesPossiveis[0].estadoDestino;
  }
  return automatoRecebido.estadosFinais.includes(estadoAtual);
}
let resultado = false;
try {
  resultado = main(inputA, inputB);
} catch (error) {
  console.log(error);
  resultado = false;
}

console.log(resultado);
*/