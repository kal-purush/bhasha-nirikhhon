package br.com.zup.TaDentro.Usuario.exceptionUsuario;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ControllerAdvUsuario {

    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(MensagemErroUsuario.class)
    public ResponseEntity<ErroBadRequestUsuario> handlerBadRequest(MensagemErroUsuario ex){
        return new ResponseEntity<>(
                ErroBadRequestUsuario.builder()
                        .titulo("BAD REQUEST")
                        .mensagem(ex.getMessage())
                        .StatusCode(400)
                        .build(), HttpStatus.BAD_REQUEST);
    }





}