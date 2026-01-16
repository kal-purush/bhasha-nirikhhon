package com.cibertec.proyecto.integrador.controller;
import com.cibertec.proyecto.integrador.services.LoginService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
@RestController
@RequestMapping("/api")
public class LoginController {

    private final LoginService servicio;


    @Autowired
    public LoginController(LoginService servicio ) {
        this.servicio=servicio;
    }

    @GetMapping("/contador")
    public int obtenerContador() {
        return 2;
    }

}