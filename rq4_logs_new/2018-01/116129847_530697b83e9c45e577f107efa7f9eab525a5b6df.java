package com.simplezuul.simplegate;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.zuul.EnableZuulProxy;
@EnableZuulProxy
@SpringBootApplication
public class SimplegateApplication {

	public static void main(String[] args) {
		SpringApplication.run(SimplegateApplication.class, args);
	}
}