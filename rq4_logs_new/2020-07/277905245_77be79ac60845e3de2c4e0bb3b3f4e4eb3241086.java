package com.springcloudmysql;

import java.util.stream.LongStream;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.springcloudmysql.model.Contact;
import com.springcloudmysql.repository.ContactRepository;

@SpringBootApplication
public class ContactServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(ContactServiceApplication.class, args);
	}

	@Bean
	CommandLineRunner init(ContactRepository repository) {
		return args -> {
			repository.deleteAll();
			LongStream.range(1, 11)
					.mapToObj(i -> new Contact(i, "Contact " + i, "contact" + i + "@email.com", "(111) 111-1111"))
					.map(v -> repository.save(v)).forEach(System.out::println);
		};

	}

}