package com.algaworks.algafood;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller //diz que é uma classe responsável por receber requisições WEB
public class MeuPrimeiroController {

	@GetMapping("/hello")
	@ResponseBody // indica que o retorno deve ser como resposta
	public String hello() {
		return "Hello!";
	}
}