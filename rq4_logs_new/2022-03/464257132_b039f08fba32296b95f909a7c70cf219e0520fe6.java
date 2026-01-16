package com.ynz.demo.jpa;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ContextConfiguration(classes = {JPAConfiguration.class})
@ExtendWith(SpringExtension.class)
class BookRepositoryTest {
  @Autowired private BookRepository bookRepository;

  @Test
  void findByName() {
    var expected = "my book";
    var book = new Book();
    book.setName(expected);
    bookRepository.save(book);

    var found = bookRepository.findByName(expected);
    assertTrue(found.isPresent());
    assertEquals(expected, found.get().getName());
  }
}