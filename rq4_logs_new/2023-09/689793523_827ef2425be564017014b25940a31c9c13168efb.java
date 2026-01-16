package com.tmm.eucurtokart.entities;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class ParticipanteEvento {

    private static final long serialVersionUID = 1L;

    private Long id;
    private Evento evento;
    private Usuario usuario;
    private boolean confirmado;
    private LocalDate dataAtualizacao;
    private LocalDate dataCadastro;
    private boolean ativo;

    @JsonIgnore
    private List<ResultadoEvento> resultadosEvento = new ArrayList<>();

    public ParticipanteEvento() {

    }

    public ParticipanteEvento(Long id, Evento evento, Usuario usuario, boolean confirmado, LocalDate dataAtualizacao,
            LocalDate dataCadastro, boolean ativo) {
        this.id = id;
        this.evento = evento;
        this.usuario = usuario;
        this.confirmado = confirmado;
        this.dataAtualizacao = dataAtualizacao;
        this.dataCadastro = dataCadastro;
        this.ativo = ativo;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Evento getEvento() {
        return evento;
    }

    public void setEvento(Evento evento) {
        this.evento = evento;
    }

    public Usuario getUsuario() {
        return usuario;
    }

    public void setUsuario(Usuario usuario) {
        this.usuario = usuario;
    }

    public boolean isConfirmado() {
        return confirmado;
    }

    public void setConfirmado(boolean confirmado) {
        this.confirmado = confirmado;
    }

    public LocalDate getDataAtualizacao() {
        return dataAtualizacao;
    }

    public void setDataAtualizacao(LocalDate dataAtualizacao) {
        this.dataAtualizacao = dataAtualizacao;
    }

    public LocalDate getDataCadastro() {
        return dataCadastro;
    }

    public void setDataCadastro(LocalDate dataCadastro) {
        this.dataCadastro = dataCadastro;
    }

    public boolean isAtivo() {
        return ativo;
    }

    public void setAtivo(boolean ativo) {
        this.ativo = ativo;
    }

    public List<ResultadoEvento> getResultadosEvento() {
        return resultadosEvento;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ParticipanteEvento other = (ParticipanteEvento) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        return true;
    }

}