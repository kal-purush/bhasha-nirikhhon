package com.lucamusic.order.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * ForceErrorRestController
 * Controller para los errores
 * 
 * @author Jose Antonio
 * @version 1.0 Septiembre 2021
 */
@RestController
public class ForceErrorRestController {
	
	@GetMapping(value= "events/throwException")
	public void throwException() {
		throw new IllegalArgumentException("\"mensaje de error del Rest Controller\"");
	}
}