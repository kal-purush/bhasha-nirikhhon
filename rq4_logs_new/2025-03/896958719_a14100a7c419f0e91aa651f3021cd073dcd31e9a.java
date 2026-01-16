package ru.otus.hw.services.impl;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.test.context.support.WithMockUser;
import ru.otus.hw.dto.BookCreateDto;
import ru.otus.hw.dto.BookDto;
import ru.otus.hw.mappers.AuthorMapper;
import ru.otus.hw.mappers.BookMapper;
import ru.otus.hw.mappers.GenreMapper;
import ru.otus.hw.models.Author;
import ru.otus.hw.models.Book;
import ru.otus.hw.models.Genre;
import ru.otus.hw.repositories.AuthorRepository;
import ru.otus.hw.repositories.BookRepository;
import ru.otus.hw.repositories.GenreRepository;
import ru.otus.hw.security.SecurityConfiguration;
import ru.otus.hw.services.BookService;

import javax.sql.DataSource;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@WebMvcTest
@Import({SecurityConfiguration.class, BookServiceImpl.class, AuthorServiceImpl.class,
        GenreServiceImpl.class, BookMapper.class, AuthorMapper.class, GenreMapper.class, BookRepository.class})
@AutoConfigureMockMvc
class BookServiceImplTest {

    @Autowired
    private BookService bookService;
    @MockBean
    private AuthorServiceImpl authorService;
    @MockBean
    private GenreServiceImpl genreService;

    @MockBean
    private BookRepository bookRepository;

    @MockBean
    private AuthorRepository authorRepository;

    @MockBean
    private GenreRepository genreRepository;

    @Autowired
    private BookMapper bookMapper;
    @Autowired
    private AuthorMapper authorMapper;
    @Autowired
    private GenreMapper genreMapper;

    @MockBean
    private DataSource dataSource;

    private static final String AUTHOR_FULL_NAME = "Author_1";
    private static final Long AUTHOR_ID = 1L;
    private static final Long GENRE_ID = 1L;
    private static final String GENRE = "Genre_1";


    @WithMockUser(
            username = "user",
            authorities = {"ROLE_ADMIN"}
    )
    @Test
    void shouldCreateNewBookWithRoleAdmin() {

        Author author = new Author(1L, AUTHOR_FULL_NAME);
        Genre genre = new Genre(1L, GENRE);
        BookCreateDto bookForSave = new BookCreateDto(1L, "Test book", 1L, 1L);
        Book book = new Book(1L, "Test book", author, genre);
        BookDto expectedBookDto = new BookDto(
                1L, "Test book", authorMapper.toDto(author), genreMapper.toDto(genre));

        when(authorRepository.findById(AUTHOR_ID)).thenReturn(Optional.of(author));
        when(genreRepository.findById(GENRE_ID)).thenReturn(Optional.of(genre));
        when(bookRepository.save(any(Book.class))).thenReturn(book);

        BookDto bookAfterSave = bookService.create(bookForSave);
        assertThat(bookAfterSave)
                .usingRecursiveComparison()
                .isEqualTo(expectedBookDto);
    }

    @WithMockUser(
            username = "user",
            authorities = {"ROLE_USER"}
    )
    @Test
    void shouldReturn401ForCreateBook() {
        BookCreateDto bookCreateDto = new BookCreateDto(1L, "Trest book", 1L, 1L);
        assertThrows(AccessDeniedException.class, ()->bookService.create(bookCreateDto));
    }
}