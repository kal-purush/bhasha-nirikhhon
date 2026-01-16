package com.seblong.okr;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.cache.annotation.EnableCaching;

@EnableAutoConfiguration
@EnableCaching
@SpringBootApplication
public class SnailOkrApplication  extends SpringBootServletInitializer{

	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
		return builder.sources(SnailOkrApplication.class);
	}

	public static void main(String[] args) {
		SpringApplication.run(SnailOkrApplication.class, args);
	}
}