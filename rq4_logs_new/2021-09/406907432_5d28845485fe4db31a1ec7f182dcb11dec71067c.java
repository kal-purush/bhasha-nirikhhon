package br.com.zup.TaDentro.colaborador.dtos;

import br.com.zup.TaDentro.enums.Cargo;

import java.time.LocalDate;

public class ColaboradorPUTDto {

    private int id;
    private Cargo cargo;
    private LocalDate dataContratacao;

    public ColaboradorPUTDto() {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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