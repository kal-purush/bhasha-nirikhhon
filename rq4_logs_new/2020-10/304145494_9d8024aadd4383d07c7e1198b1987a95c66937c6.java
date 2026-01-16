package wolox.training.repositories;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.junit4.SpringRunner;
import wolox.training.models.Book;
import wolox.training.util.TestEntities;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

@RunWith(SpringRunner.class)
@DataJpaTest
public class BookRepositoryTest {

    @Autowired
    private BookRepository bookRepository;

    private static Book oneTestBook;

    @BeforeAll
    static void setUp() {
        oneTestBook = TestEntities.mockBook();
    }

    @Test
    void whenSaveBookThenJpaPersisted() {

        oneTestBook.setId(1L);
        oneTestBook.setGenre("genre");
        oneTestBook.setAuthor("author");
        oneTestBook.setImage("image");
        oneTestBook.setTitle("title");
        oneTestBook.setSubtitle("subtitle");
        oneTestBook.setPublisher("publisher");
        oneTestBook.setYear("222");
        oneTestBook.setPages("22");
        oneTestBook.setIsbn("22");

        bookRepository.save(oneTestBook);

        Optional<Book> book = bookRepository.findById(1L);
        assertEquals(book.get().getId(), oneTestBook.getId());
    }
}