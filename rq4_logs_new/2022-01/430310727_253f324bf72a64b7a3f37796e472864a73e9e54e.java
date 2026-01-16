package ru.otus.spring.mongock;

import com.github.cloudyrock.mongock.ChangeSet;
import com.mongodb.client.MongoDatabase;
import ru.otus.spring.domain.Author;
import ru.otus.spring.domain.Book;
import ru.otus.spring.domain.Genre;
import ru.otus.spring.repositories.AuthorRepository;
import ru.otus.spring.repositories.BookRepository;
import ru.otus.spring.repositories.GenreRepository;

@com.github.cloudyrock.mongock.ChangeLog
public class ChangeLog {

    private final Author author1 = new Author("Ivanov Petr Olegovich");
    private final Author author2 = new Author("Petrov Kirill Fedorovich");
    private final Genre genre1 = new Genre("Non fiction");
    private final Genre genre2 = new Genre("Roman");
    private final Book book1 = new Book("title1", author1, genre1);
    private final Book book2 = new Book("title2", author2, genre2);


    @ChangeSet(order = "001", id = "dropDb", author = "filg", runAlways = true)
    public void dropDb(MongoDatabase db) {
        db.drop();
    }


    @ChangeSet(order = "002", author = "filg", id = "initAuthors")
    public void initAuthors(AuthorRepository authorRepository) {
        authorRepository.save(author1);
        authorRepository.save(author2);
    }

    @ChangeSet(order = "003", id = "initGenres", author = "filg")
    public void initGenres(GenreRepository repository) {
        repository.save(genre1);
        repository.save(genre2);
    }

    @ChangeSet(order = "004", id = "initBooks", author = "filg")
    public void initBooks(BookRepository repository) {
        repository.save(book1);
        repository.save(book2);
    }


}