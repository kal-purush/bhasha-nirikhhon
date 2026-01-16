package br.com.ardaexperience.modelo;

import java.util.Date;

public class CartaoCredito {
    
    private Long id;
    
    private String nomeCliente;
    
    private Long numeroCartao;
    
    private int numeroSeguranca;
    
    private Date mesExpiracao;
    
    private Date anoExpiracao;

    public CartaoCredito() {
    
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getNomeCliente() {
        return nomeCliente;
    }

    public void setNomeCliente(String nomeCliente) {
        this.nomeCliente = nomeCliente;
    }

    public Long getNumeroCartao() {
        return numeroCartao;
    }

    public void setNumeroCartao(Long numeroCartao) {
        this.numeroCartao = numeroCartao;
    }

    public int getNumeroSeguranca() {
        return numeroSeguranca;
    }

    public void setNumeroSeguranca(int numeroSeguranca) {
        this.numeroSeguranca = numeroSeguranca;
    }

    public Date getMesExpiracao() {
        return mesExpiracao;
    }

    public void setMesExpiracao(Date mesExpiracao) {
        this.mesExpiracao = mesExpiracao;
    }

    public Date getAnoExpiracao() {
        return anoExpiracao;
    }

    public void setAnoExpiracao(Date anoExpiracao) {
        this.anoExpiracao = anoExpiracao;
    }
    
}