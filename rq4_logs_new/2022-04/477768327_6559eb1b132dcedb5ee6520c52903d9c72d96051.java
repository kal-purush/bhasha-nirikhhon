package com.fundamentos.fundamentos.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class TestController {
    // @RequestMapping Acepta todas las solicitudes dentro de este metodo a nivel http
    // @ResponseBody Para responder un cuerpo a nivel del servicio
    @ResponseBody
    @RequestMapping
    public ResponseEntity<String> function() {
                return new ResponseEntity<>("Hello from controller cambio", HttpStatus.OK);
    }
}