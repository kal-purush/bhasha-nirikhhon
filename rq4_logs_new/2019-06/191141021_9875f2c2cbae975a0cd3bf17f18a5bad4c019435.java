package com.tutorialspoint.RestReq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@RestController
public class RestReqApplication {

	public static void main(String[] args) {
		SpringApplication.run(RestReqApplication.class, args);
	}
	
	@RequestMapping(value="/")
	public String hello(){
		 return "Hello World";
	 }
	
	@Bean
	public RestTemplate getRestTemplate(){
		return new RestTemplate();
	}
	

}