package dev.prpires66.controller;

import dev.prpires66.domain.model.Funcionario;
import dev.prpires66.domain.model.LoginResponse;
import dev.prpires66.service.FuncionarioService;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/login")
public class LoginController {

    private final FuncionarioService funcionarioService;

    public LoginController(FuncionarioService funcionarioService) {
        this.funcionarioService = funcionarioService;
    }
    @PostMapping
    public ResponseEntity<LoginResponse> login(@RequestBody Funcionario funcionarioToLogin) {
        LoginResponse retorno = funcionarioService.login(funcionarioToLogin).getBody();

        if(retorno.isAuthenticated()) {
            return ResponseEntity.status(HttpStatus.OK).body(retorno);
        } else {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(retorno);
        }
    }
}