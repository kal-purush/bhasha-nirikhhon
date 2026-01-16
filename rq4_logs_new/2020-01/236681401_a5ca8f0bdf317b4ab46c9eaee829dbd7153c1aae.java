package com.att.edf;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ApiService {

	@GetMapping("/getres")
	public String getRes() {
		System.out.println("Api is called");
		return "Api called";
	}
}