package br.com.zup.TaDentro.indicacao.dtos;

import br.com.zup.TaDentro.enums.PerfilDeSituacao;

import java.time.LocalDate;

public class IndicacaoPUTDto {

    private String cpf;
    private PerfilDeSituacao situacao;
    private LocalDate dataDaContratacao;

    public String getCpf() {
        return cpf;
    }

    public void setCpf(String cpf) {
        this.cpf = cpf;
    }

    public PerfilDeSituacao getSituacao() {
        return situacao;
    }

    public void setSituacao(PerfilDeSituacao situacao) {
        this.situacao = situacao;
    }

    public LocalDate getDataDaContratacao() {
        return dataDaContratacao;
    }

    public void setDataDaContratacao(LocalDate dataDaContratacao) {
        this.dataDaContratacao = dataDaContratacao;
    }
}