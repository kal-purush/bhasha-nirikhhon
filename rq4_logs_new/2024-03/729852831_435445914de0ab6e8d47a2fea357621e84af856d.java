package mmk.crud;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;

@SpringBootApplication
@EnableJpaRepositories
@OpenAPIDefinition
public class SpringCrudApplication{

	public static void main(String[] args) {
		SpringApplication.run(SpringCrudApplication.class, args);
	}
	
	
	@Bean
	CommandLineRunner commandLineRunner(String[] args) {
		return runner -> {
			System.out.println("Hello Spring");
			
			
		};
	}
	
}