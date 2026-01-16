package br.com.fiap.soat.grupo48.application.pedido.exception;

import br.com.fiap.soat.grupo48.application.commons.exception.ApplicationException;

public class MetodoPagamentoInvalidoException extends ApplicationException {

    private static final long serialVersionUID = -5702077678463666943L;

    public MetodoPagamentoInvalidoException(String message) {
        super(message);
    }

    public MetodoPagamentoInvalidoException(String message, Throwable cause) {
        super(message, cause);
    }
}