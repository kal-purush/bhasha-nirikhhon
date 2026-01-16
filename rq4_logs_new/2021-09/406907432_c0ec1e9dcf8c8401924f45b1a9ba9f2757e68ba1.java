package br.com.zup.TaDentro.indicacao.exceptionIndicacao;

import lombok.Builder;

@Builder
public class ErroBadRequestIndicacao {

    private int StatusCode;
    private String mensagem;
    private String titulo;

    public int getStatusCode() {
        return StatusCode;
    }

    public void setStatusCode(int statusCode) {
        StatusCode = statusCode;
    }

    public String getMensagem() {
        return mensagem;
    }

    public void setMensagem(String mensagem) {
        this.mensagem = mensagem;
    }

    public String getTitulo() {
        return titulo;
    }

    public void setTitulo(String titulo) {
        this.titulo = titulo;
    }
}