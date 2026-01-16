export interface Pessoa {
    nome: String;
}

export class Lancamento {
    pessoa: Pessoa;
    codigo: Number;
    descricao: String;
    dataVencimento: Date;
    dataPagamento: Date;
    valor: Number;

    
}