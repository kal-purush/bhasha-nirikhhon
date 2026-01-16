package com.java.codeFront.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class PageController {

	// Página de inicio
	@GetMapping("/")
	public String home(Model model) {
		return "home"; // Devuelve la plantilla home.html
	}

	// Página de "Acerca de"
	@GetMapping("/login")
	public String about(Model model) {
		return "login"; // Devuelve la plantilla about.html
	}

	// Página de contacto
	@GetMapping("/favorites")
	public String contact(Model model) {
		return "favorites"; // Devuelve la plantilla contact.html
	}
}
