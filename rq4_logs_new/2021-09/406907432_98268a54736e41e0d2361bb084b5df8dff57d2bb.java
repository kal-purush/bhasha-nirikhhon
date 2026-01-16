package br.com.zup.TaDentro.exeption;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.HashMap;

@RestControllerAdvice
public class ControllerAdvisor {

    @ExceptionHandler(AcessoNegado.class)
    public ResponseEntity<?> handlerExption(AcessoNegado exception){
        HashMap<String, String> mesagem = new HashMap<>();
        mesagem.put("mensagemErro", exception.getMessage());

        return ResponseEntity.status(exception.getStatusCode()).body(mesagem);

    }



}