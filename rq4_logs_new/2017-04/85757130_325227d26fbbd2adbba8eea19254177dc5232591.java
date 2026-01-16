package ru.ncedu.purchasebot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import ru.ncedu.purchasebot.model.Customer;
import ru.ncedu.purchasebot.repository.CustomerRepository;

@SpringBootApplication
public class TestApplication {

	private static final Logger log = LoggerFactory.getLogger(TestApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(TestApplication.class);
	}

	@Bean
	public CommandLineRunner demo(CustomerRepository repository) {
		return (args) -> {
			repository.save(new Customer("guest", "123", "guest@anymail.com", 100));

			log.info("Customers found with findAll():");
			log.info("-------------------------------");
			for (Customer customer : repository.findAll()) {
				log.info(customer.toString());
				repository.delete(customer);
			}
			log.info("");

		};
	}

}