package mate.academy;

import java.math.BigDecimal;
import mate.academy.model.Book;
import mate.academy.service.BookService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class BookStoreApplication {
    @Autowired
    private BookService bookService;

    public static void main(String[] args) {
        SpringApplication.run(BookStoreApplication.class, args);
    }

    @Bean
    public CommandLineRunner commandLineRunner() {
        return args -> {
            Book harryPotter = new Book();
            harryPotter.setAuthor("J. K. Rowling");
            harryPotter.setTitle("Harry Potter and the Philosopher Stone");
            harryPotter.setPrice(BigDecimal.TEN);
            harryPotter.setDescription("First part of bestselling series");
            harryPotter.setIsbn("978-3-16-148410-0");
            harryPotter.setCoverImage("cover");
            System.out.println(bookService.save(harryPotter));
            System.out.println(bookService.findAll());
        };
    }
}