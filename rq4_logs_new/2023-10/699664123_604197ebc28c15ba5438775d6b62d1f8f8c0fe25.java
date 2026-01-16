package com.rahulsharan.java_app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class JavaAppApplication {
	@GetMapping("/message")
	public String getMessage() {
		return "<html><body style='background-color: blue; display: flex; justify-content: center; align-items: center; height: 100vh;'><h1 style='color: white;'>Java Application is working!</h1></body></html>";
	}

	public static void main(String[] args) {
		SpringApplication.run(JavaAppApplication.class, args);
	}

}