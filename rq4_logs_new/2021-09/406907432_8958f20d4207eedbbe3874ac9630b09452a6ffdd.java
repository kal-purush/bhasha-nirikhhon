package br.com.zup.TaDentro.jwt.exeptionAuthentication;

import br.com.zup.TaDentro.Usuario.exceptionUsuario.MensagemErroUsuario;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ControllerAdvisorFilters {

    @ResponseStatus(HttpStatus.FORBIDDEN)
    @ExceptionHandler(MensagemAcessoNegado.class)
    public ResponseEntity<AcessoNegadoException> capturaDeExcecao(MensagemAcessoNegado exception){
        return new ResponseEntity<>(
                AcessoNegadoException.builder()
                .statusCode(403)
                .mensagem(exception.getMessage())

                .titulo("FORBIDDEN")
                .build(), HttpStatus.FORBIDDEN
        );
    }

    @ResponseStatus(HttpStatus.UNAUTHORIZED)
    @ExceptionHandler(MensagemTokenInvalido.class)
    public ResponseEntity<TokenInvalidoException> capturaTokenInvalido(MensagemTokenInvalido exceptionToke){
        return new ResponseEntity(
                TokenInvalidoException.builder()
                .statusCode(401)
                .mensagem("TOKEN INVALID")
                .titulo("Unauthorized")
                .build(), HttpStatus.UNAUTHORIZED

        );
    }




}