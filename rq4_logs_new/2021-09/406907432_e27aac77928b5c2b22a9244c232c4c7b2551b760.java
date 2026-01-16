package br.com.zup.TaDentro.colaborador.dtos;

import br.com.zup.TaDentro.enums.Cargo;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.time.LocalDate;

public class ColaboradorResumidoDTO {

    @NotNull
    private String nome;
    @NotNull
    @Enumerated(EnumType.STRING)
    private Cargo cargo;
    @NotNull
    private LocalDate dataContratacao;

    public ColaboradorResumidoDTO() {
    }

    public ColaboradorResumidoDTO(String nome, Cargo cargo, LocalDate dataContratacao) {
        this.nome = nome;
        this.cargo = cargo;
        this.dataContratacao = dataContratacao;
    }

    public String getNome() {
        return nome;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    public Cargo getCargo() {
        return cargo;
    }

    public void setCargo(Cargo cargo) {
        this.cargo = cargo;
    }

    public LocalDate getDataContratacao() {
        return dataContratacao;
    }

    public void setDataContratacao(LocalDate dataContratacao) {
        this.dataContratacao = dataContratacao;
    }

}