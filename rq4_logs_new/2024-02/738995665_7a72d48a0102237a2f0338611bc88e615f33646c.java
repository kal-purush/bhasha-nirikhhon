package com.example.prometheusNNgrafana.Controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ApiController {
	@PostMapping("/api/postresource")
	public String getPostResource() {
		return "Post Response";
	}

}