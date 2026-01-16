package com.csaba79coder.bookliquibase.domain.book.bootstrap;

import com.csaba79coder.bookliquibase.domain.book.entity.Book;
import com.csaba79coder.bookliquibase.domain.book.persistence.BookRepository;
import com.csaba79coder.bookliquibase.domain.book.value.Availability;
import com.csaba79coder.bookliquibase.domain.book.value.Genre;
import com.csaba79coder.bookliquibase.domain.book.value.Status;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@RequiredArgsConstructor
public class DataLoader implements ApplicationRunner {

    private final BookRepository bookRepository;
    @Override
    public void run(ApplicationArguments args) throws Exception {

        Book firstBook = new Book();
        firstBook.setId(UUID.fromString("8719d2be-30f4-4c58-9af8-8fb777ec8fd3"));
        firstBook.setTitle("Effective Java");
        firstBook.setIsbn(9780137150021L);
        firstBook.setGenre(Genre.OTHER);
        firstBook.setAvailability(Availability.AVAILABLE);
        firstBook.setStatus(Status.AVAILABLE);

        bookRepository.save(firstBook);
    }
}