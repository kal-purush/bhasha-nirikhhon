package com.example.apigatewayservice.configuration;

import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// @Configuration
public class FilterConfig {

	@Bean
	public RouteLocator gatewayRoutes(RouteLocatorBuilder builder) {

		return builder.routes()
			.route(r -> r
				.path("/first-service/**")
				.filters(f -> f
					.addRequestHeader("first-req", "first-req-val")
					.addResponseHeader("first-resp", "first-resp-val")
				)
				.uri("http://localhost:8001")
			)
			.route(r -> r
				.path("/second-service/**")
				.filters(f -> f
					.addRequestHeader("second-req", "second-req-val")
					.addResponseHeader("second-resp", "second-resp-val")
				)
				.uri("http://localhost:8002")
			)
			.build();
	}
}