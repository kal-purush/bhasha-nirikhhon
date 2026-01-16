package com.springspa;

import java.util.Arrays;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@Configuration
@ComponentScan
@EnableJpaRepositories
@EnableAutoConfiguration
public class Application {

	public static void main(String[] args) {
		ApplicationContext ctx = SpringApplication.run(Application.class, args);

		BookRepository repo = ctx.getBean(BookRepository.class);

		Book book1 = new Book();
		book1.setTitle("Programming Groovy 2");
		book1.setAuthor("Venkat Subramaniam");
		book1.setIsbn(" 978-1-93778-530-7");
		book1.setPageCount(200L);
		book1.setPublisher("Pragmatic Bookshelf");
		book1.setUrl("https://pragprog.com/book/vslg2/programming-groovy-2");

		Book book2 = new Book();
		book2.setTitle("Grails in Action");
		book2.setAuthor("Peter Ledbrook");
		book2.setIsbn(" 1933988932");
		book2.setPageCount(500L);
		book2.setPublisher("Manning");
		book2.setUrl("http://www.manning.com/gsmith/");

		repo.save(book1);
		repo.save(book2);

		System.out.println("Bootstraping database\n");
		System.out.println("*********************\n\n");

		System.out.println("Printing bean definitions..\n");

		String[] beanNames = ctx.getBeanDefinitionNames();
		Arrays.sort(beanNames);
		for (String beanName : beanNames) {
			System.out.println(beanName);

		}

	}

}