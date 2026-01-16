package ru.otus.spring.repositories.jpa;

import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.context.annotation.Import;
import ru.otus.spring.domain.Author;
import ru.otus.spring.domain.Book;
import ru.otus.spring.domain.Genre;
import ru.otus.spring.repositories.BookRepository;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Репозиторий на основе Jpa для работы с книгами ")
@DataJpaTest
@Import(BookRepositoryJpa.class)
public class BookJpaTests {
    @Autowired
    private TestEntityManager em;
    @Autowired
    private BookRepository bookRepository;

    private static final String BOOK_TITLE1 = "title1";
    private static final String BOOK_TITLE = "Korgi dog";
    private static final String GENRE_NAME = "biographic";
    private static final String AUTHORS_NAME = "Quine";
    private static final long AUTHORS_ID = 3;
    private static final long GENRE_ID = 3;
    private static final long TEST_BOOK_ID = 3;
    private static final long FIRST_BOOK_ID = 1;

    @Test
    @DisplayName("должен создавать книги")
    public void testCreation() {
        long countBooks = bookRepository.countBooks();

        Book book = new Book(0, BOOK_TITLE, getTestAuthor(), getTestGenre());
        Book savedBook = bookRepository.save(book);
        long countBooksAfter = bookRepository.countBooks();

        assertThat(savedBook.getId()).isGreaterThan(0);

        val actualBook = em.find(Book.class, savedBook.getId());
        assertThat(actualBook).isNotNull().matches(b -> b.getGenre().equals(getTestGenre()))
                .matches(b -> b.getAuthor().equals(getTestAuthor()))
                .matches(b -> b.getId() == TEST_BOOK_ID);
        assertThat(countBooksAfter).isEqualTo(countBooks + 1);
    }

    @Test
    @DisplayName("должен редактировать книги")
    public void testEdit() {
        Book bookBefore = em.find(Book.class, 2l);
        String bookBeforeTitle = bookBefore.getTitle();
        Book editBook = new Book(2l, BOOK_TITLE, getTestAuthor(), getTestGenre());
        bookRepository.save(editBook);
        Book bookAfter = em.find(Book.class, 2l);
        Assertions.assertNotEquals(bookBeforeTitle, bookAfter.getTitle());
        assertThat(bookAfter).isEqualTo(editBook);
    }

    @Test
    @DisplayName("должен считать книги")
    public void testCount() {
        long countBooks = bookRepository.countBooks();
        Assertions.assertEquals(2l, countBooks);
    }

    @Test
    @DisplayName("должен отдавать книги по идентификатору")
    public void testGetById() {
        Book book = bookRepository.getBookById(1l).get();
        assertThat(book).isNotNull().matches(b->b.getId()==1)
                .matches(b->b.getTitle().equals(BOOK_TITLE1));
    }

    @Test
    @DisplayName("должен отдавать книги по автору")
    public void testGetByAuthor() {
        Book book = bookRepository.getBooksByAuthor(getTestAuthor()).get(0);
        assertThat(book).isNotNull().matches(b->b.getId()==1)
                .matches(b->b.getTitle().equals(BOOK_TITLE1));
    }

    @Test
    @DisplayName("должен удалять книги")
    public void testDelete() {
        val firstBook = em.find(Book.class, FIRST_BOOK_ID);
        assertThat(firstBook).isNotNull();

        em.detach(firstBook);
        bookRepository.delete(FIRST_BOOK_ID);

        val deletedBook = em.find(Book.class, FIRST_BOOK_ID);

        assertThat(deletedBook).isNull();
    }

    private Author getTestAuthor() {
        return new Author(AUTHORS_ID, AUTHORS_NAME);
    }

    private Genre getTestGenre() {
        return new Genre(GENRE_ID, GENRE_NAME);
    }
}