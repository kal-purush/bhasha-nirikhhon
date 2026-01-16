package br.com.zup.TaDentro.jwt.exeptionAuthentication;

import lombok.Builder;

@Builder
public class TokenInvalidoException {

    private int statusCode;
    private String mensagem;
    private String titulo;

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
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