package tan.com.fibonacci.app.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
public class FibonacciAppServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(FibonacciAppServiceApplication.class, args);
	}
	
	@Autowired
	private RestTemplateBuilder restTemplateBuilder;
	
	
	@Bean
	public RestTemplate restTemplate(RestTemplateBuilder restTemplateBuilder) {
		return restTemplateBuilder.build();
	}

}