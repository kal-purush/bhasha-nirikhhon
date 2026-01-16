package com.helloworld.hello.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/Hello")

public class HelloController {

	@GetMapping
	public String hello() {
		return "Hello Generation <3";
	}
	@GetMapping("/Atividade1")
	public String hello1() {
	return "Habilidade e Mentalidades: Persistência e Atenção aos detalhes.";
	}
		
	@GetMapping("/Atividade2")
	public String hello2() {
	return "Meus objetivos para semana são: Melhorar minhas técnicas no MySQL e Spring.";
	}
	
}