package br.com.dinheiro.irpf.aplicacao.impl;

import lombok.*;

import java.util.Date;
import java.util.List;

@Data
@NoArgsConstructor
public class NegociacaoDTO {
    private String nomeCliente;
    private String cpf;
    private String idCliente;
    private String dataNegociacao;
    private String taxaLiquidacao;
    private String emonumentos;
    private String irrf;
    private List<OperacaoDto> operacao;
    private String numeroNota;


}