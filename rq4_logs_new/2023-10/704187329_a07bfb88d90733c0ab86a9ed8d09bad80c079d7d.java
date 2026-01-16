package br.com.serratec.ecommerce.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;

import java.util.Date;

@Entity
public class Auditoria {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    @Column(nullable = false)
    private ETipoEntidade entidade;

    @Column(nullable = false)
    private String operacao;

    private String valorOriginal;

    private String valorAtualizado;

    @Column(nullable = false)
    private Date dataDaOperacao;

    @ManyToOne
    @JoinColumn(name = "usuario_id", nullable = false)
    private Usuario usuario;

    public Auditoria(ETipoEntidade produto, String string, String string2, Produto produto2) {
        dataDaOperacao = new Date();
    }

    public Auditoria(ETipoEntidade entidade, String operacao, String valorOriginal, String valorAtualizado,
             Usuario usuario) {
        this.entidade = entidade;
        this.operacao = operacao;
        this.valorOriginal = valorOriginal;
        this.valorAtualizado = valorAtualizado;
        this.usuario = usuario;
        this.dataDaOperacao = new Date();
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public ETipoEntidade getEntidade() {
        return entidade;
    }

    public void setEntidade(ETipoEntidade entidade) {
        this.entidade = entidade;
    }

    public String getOperacao() {
        return operacao;
    }

    public void setOperacao(String operacao) {
        this.operacao = operacao;
    }

    public String getValorOriginal() {
        return valorOriginal;
    }

    public void setValorOriginal(String valorOriginal) {
        this.valorOriginal = valorOriginal;
    }

    public String getValorAtualizado() {
        return valorAtualizado;
    }

    public void setValorAtualizado(String valorAtualizado) {
        this.valorAtualizado = valorAtualizado;
    }

    public Date getDataDaOperacao() {
        return dataDaOperacao;
    }

    public void setDataDaOperacao(Date dataDaOperacao) {
        this.dataDaOperacao = dataDaOperacao;
    }

}