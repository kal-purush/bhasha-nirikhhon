package ru.otus.spring.shell;

import lombok.RequiredArgsConstructor;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
import ru.otus.spring.domain.Book;
import ru.otus.spring.domain.Comment;
import ru.otus.spring.repositories.AuthorRepository;
import ru.otus.spring.repositories.BookRepository;
import ru.otus.spring.repositories.CommentRepository;
import ru.otus.spring.repositories.GenreRepository;

import javax.transaction.Transactional;

@ShellComponent
@RequiredArgsConstructor
public class ShellApplicationAdd {
    private final AuthorRepository authorService;
    private final BookRepository bookService;
    private final GenreRepository genreService;
    private final CommentRepository commentRepository;


    @ShellMethod(value = "add book", key = {"addB"})
    @Transactional
    public void addBook(@ShellOption(defaultValue = "Title") String title,
                        @ShellOption(defaultValue = "0") String authorId,
                        @ShellOption(defaultValue = "0") String genreId) {
        long id = bookService.countBooks() + 1;
        Book book = new Book(id,
                title,
                authorService.getAuthors().get(0),
//                authorService.getAuthorById(Long.parseLong(authorId)).get(),
                genreService.getGenres().get(Integer.parseInt(genreId)));
        bookService.save(book);
    }

    @ShellMethod(value = "add books comment", key = {"addBC"})
    @Transactional
    public void addBooksComment(@ShellOption(defaultValue = "1") String bookId,
                                @ShellOption(defaultValue = "good Book") String commentString) {
        Comment comment = new Comment(Long.valueOf(bookId), commentString);
        commentRepository.save(comment);
    }

}