package br.com.zup.TaDentro.indicacao.dtos;

import java.time.LocalDate;

public class IndicacaoPesquisaDTO {

    private LocalDate dataInicial;
    private LocalDate dataFinal;

    public IndicacaoPesquisaDTO() {
    }

    public LocalDate getDataInicial() {
        return dataInicial;
    }

    public void setDataInicial(LocalDate dataInicial) {
        this.dataInicial = dataInicial;
    }

    public LocalDate getDataFinal() {
        return dataFinal;
    }

    public void setDataFinal(LocalDate dataFinal) {
        this.dataFinal = dataFinal;
    }
}