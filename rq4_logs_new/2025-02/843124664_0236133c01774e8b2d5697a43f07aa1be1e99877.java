package local.pms.apigateway.config;

import org.springframework.cloud.gateway.route.RouteLocator;

import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Configuration;

@Profile("docker")
@Configuration
public class ApiGatewayDockerConfig {

    @Bean
    public RouteLocator configureRoute(RouteLocatorBuilder builder) {
        return builder.routes()
                .route("auth-service", r -> r.path("/api/v1/auth/**")
                        .uri("lb://AUTH-SERVICE"))
                .route("user-service", r -> r.path("/api/v1/users/**")
                        .uri("lb://USER-SERVICE"))
                .route("project-service", r -> r.path("/api/v1/projects/**")
                        .uri("lb://PROJECT-SERVICE"))
                .route("task-service", r -> r.path("/api/v1/task/**")
                        .uri("lb://TASK-SERVICE"))
                .build();
    }
}