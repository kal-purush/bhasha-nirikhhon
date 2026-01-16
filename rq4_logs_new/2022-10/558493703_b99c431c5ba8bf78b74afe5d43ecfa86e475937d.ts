export type automato = {
  estados: string[];
  alfabeto: string[];
  transicoes: transicao[];
  estadoInicial: string;
  estadosFinais: string[];
};

export type transicao = {
  estadoAtual: string;
  simbolo: string;
  estadoDestino: string;
};