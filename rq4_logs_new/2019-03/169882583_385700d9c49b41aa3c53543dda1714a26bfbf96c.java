package ru.aaxee.homework2.repository;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.jdbc.Sql;
import ru.aaxee.homework2.domain.Author;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.context.jdbc.Sql.ExecutionPhase.AFTER_TEST_METHOD;
import static org.springframework.test.context.jdbc.Sql.ExecutionPhase.BEFORE_TEST_METHOD;

@JdbcTest
@ComponentScan
@Sql(executionPhase = BEFORE_TEST_METHOD, scripts = "classpath:author_testdata.sql")
@Sql(executionPhase = AFTER_TEST_METHOD, scripts = "classpath:author_clear.sql")
class AuthorJdbcRepositoryTest {

    @Autowired
    AuthorRepository authorRepository;

    @Test
    void addAuthor() {
        String name = "Author1";
        authorRepository.addAuthor(name);
        Optional<Author> optionalAuthor = authorRepository.findByName(name);
        assertThat(optionalAuthor).isNotEmpty();
    }

    @Test
    void findById() {
        Optional<Author> optionalAuthor = authorRepository.findById(1L);
        assertThat(optionalAuthor).isNotEmpty();
    }

    @Test
    void findAll() {
        List<Author> authors = authorRepository.findAll();
        assertThat(authors).hasSize(3);
    }

    @Test
    void updateAuthor() {
        Long id = 1L;
        String newName = "New name";
        authorRepository.updateAuthor(id, newName);
        Optional<Author> optionalAuthor = authorRepository.findById(id);
        assertThat(optionalAuthor).isPresent();
        assertThat(optionalAuthor.get().getName()).isEqualTo(newName);
    }

    @Test
    void deleteById() {
        Long id = 1L;
        Optional<Author> before = authorRepository.findById(id);
        assertThat(before).isPresent();

        authorRepository.deleteById(id);

        Optional<Author> after = authorRepository.findById(id);
        assertThat(after).isEmpty();
    }

    @Test
    void findByName() {
        Optional<Author> optionalAuthor = authorRepository.findByName("Pushkin");
        assertThat(optionalAuthor).isPresent();
    }
}