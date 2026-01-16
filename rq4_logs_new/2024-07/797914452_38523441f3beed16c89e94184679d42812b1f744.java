package com.codeconnect.usuario.exception;

import com.codeconnect.exceptionhandler.CodeConnectException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;

public class ErroAoAdicionarImagemUsuarioException extends CodeConnectException {

    @Override
    public ProblemDetail handleProblemDetail() {
        ProblemDetail detalheProblema = ProblemDetail.forStatus(HttpStatus.BAD_REQUEST);

        detalheProblema.setTitle("Erro ao adicionar imagem do usuário");

        return detalheProblema;
    }
}