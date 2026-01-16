package com.rafaeldeluca.Ecommerce.Java.crontrollers;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

// um controladdor implementa um recurso na API REST
// funcionalidades de um sistema web são organizadas em forma de recursos URL
// URL = universal resource locator
@RestController
@RequestMapping (value = "/produtos")
public class ProductController {

    @GetMapping
    public String testeEndPoint (){
        return "Testar End Point /produtos";
    }

}