package br.com.zup.TaDentro.indicacao.dtos;

import br.com.zup.TaDentro.enums.PerfilDeSituacao;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.time.LocalDate;

public class IndicacaoResumidaDTO {


    @NotNull
    private String nome;
    @NotNull
    private LocalDate dataDaContratacao;
    private String email;
    @NotNull
    @Enumerated(EnumType.STRING)
    private PerfilDeSituacao situacao;

    public IndicacaoResumidaDTO() {
    }

    public IndicacaoResumidaDTO(String nome, LocalDate dataDaContratacao, String email, PerfilDeSituacao situacao) {
        this.nome = nome;
        this.dataDaContratacao = dataDaContratacao;
        this.email = email;
        this.situacao = situacao;
    }

    public String getNome() {
        return nome;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    public LocalDate getDataDaContratacao() {
        return dataDaContratacao;
    }

    public void setDataDaContratacao(LocalDate dataDaContratacao) {
        this.dataDaContratacao = dataDaContratacao;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public PerfilDeSituacao getSituacao() {
        return situacao;
    }

    public void setSituacao(PerfilDeSituacao situacao) {
        this.situacao = situacao;
    }
}