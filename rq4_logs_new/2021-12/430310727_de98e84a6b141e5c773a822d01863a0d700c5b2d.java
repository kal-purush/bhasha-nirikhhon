package ru.otus.spring;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.EmptyResultDataAccessException;
import ru.otus.spring.dao.jdbc.BookDaoJdbc;
import ru.otus.spring.domain.Author;
import ru.otus.spring.domain.Book;
import ru.otus.spring.domain.Genre;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Dao для работы с книгами должно")
@JdbcTest
@Import(BookDaoJdbc.class)
public class BookDaoTest {

    @Autowired
    private BookDaoJdbc bookDao;
    private int EXPECTED_BOOK_COUNT = 2;
    private long EXISTING_BOOK_ID = 1;

    @DisplayName("возвращать ожидаемое количество книг в БД")
    @Test
    void shouldReturnExpectedBookCount() {
        int actualBookCount = bookDao.countBooks();
        assertThat(actualBookCount).isEqualTo(EXPECTED_BOOK_COUNT);
    }

    @DisplayName("добавлять книги в БД")
    @Test
    void shouldAddBook() {
        Book newBook = new Book(3, "ExpTitle", new Author(2, "Pavel IV"), new Genre(2, "dramma"));

        int countBefore = bookDao.countBooks();

        bookDao.addBook(newBook);
        int countAfter = bookDao.countBooks();

        assertThat(countAfter).isEqualTo(countBefore + 1);
    }

    @DisplayName("удалять книги в БД")
    @Test
    void shouldDeleteBook() {
        int countBefore = bookDao.countBooks();
        Book bookById = bookDao.getBookById(EXISTING_BOOK_ID);
        bookDao.deleteBook(bookById);
        int countAfter = bookDao.countBooks();

        assertThat(countAfter).isEqualTo(countBefore - 1);

        assertThatThrownBy(() -> bookDao.getBookById(EXISTING_BOOK_ID))
                .isInstanceOf(EmptyResultDataAccessException.class);
    }

    @DisplayName("изменять книги в БД")
    @Test
    void shouldUpdateBook() {
        Book newBook = new Book(EXISTING_BOOK_ID, "ExpTitle",
                new Author(1, "Ivanov Petr Olegovich"),
                new Genre(1, "Non fiction"));


        bookDao.updateBook(newBook);
        Book bookAfter = bookDao.getBookById(EXISTING_BOOK_ID);

        assertThat(bookAfter).isEqualTo(newBook);
    }
    @DisplayName("возвращать книги из БД")
    @Test
    void shouldReturnBook() {
        Book etalonBook = new Book(EXISTING_BOOK_ID, "title1",
                new Author(1, "Ivanov Petr Olegovich"),
                new Genre(1, "Non fiction"));


        Book book = bookDao.getBookById(EXISTING_BOOK_ID);

        assertThat(book).isEqualTo(etalonBook);
    }
}