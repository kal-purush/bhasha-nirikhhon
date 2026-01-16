package com.reservaciones_restaurante;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;


@SpringBootApplication
@EntityScan(basePackages = {"com.reservaciones_restaurante.model"})
@ComponentScan(basePackages = {"com.reservaciones_restaurante.config","com.reservaciones_restaurante.controller",
		"com.reservaciones_restaurante.model","com.reservaciones_restaurante.repository","com.reservaciones_restaurante.services"} )
public class ReservacionesRestauranteApplication {

	public static void main(String[] args) {
		SpringApplication.run(ReservacionesRestauranteApplication.class, args);
	}
	

}