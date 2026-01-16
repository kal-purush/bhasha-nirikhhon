package br.com.fiap.dimdim;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

import springfox.documentation.swagger2.annotations.EnableSwagger2;

@SpringBootApplication
@EnableCaching
@EnableSwagger2
public class DimDim {

	public static void main(String[] args) {
		SpringApplication.run(DimDim.class, args);
	}

}