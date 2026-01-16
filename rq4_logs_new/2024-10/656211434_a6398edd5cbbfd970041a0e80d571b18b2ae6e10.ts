
export class PagamentoIndevidoDTO  {
 
  constructor(dto: any) {
    this.id = (dto as PagamentoIndevidoDTO).id
    this.dataPagamento = (dto as PagamentoIndevidoDTO).dataPagamento;
    this.dataReferencia= (dto as PagamentoIndevidoDTO).dataReferencia;
    this.nomeFavorecido= (dto as PagamentoIndevidoDTO).nomeFavorecido;
    this.valorPago= (dto as PagamentoIndevidoDTO).valorPago;
    this.valorDebitar= (dto as PagamentoIndevidoDTO).valorDebitar;		   
  }

  id: number;
  dataPagamento: Date;
  dataReferencia: Date;
  nomeFavorecido: string;
  valorPago:number;
  valorDebitar:number;		

}