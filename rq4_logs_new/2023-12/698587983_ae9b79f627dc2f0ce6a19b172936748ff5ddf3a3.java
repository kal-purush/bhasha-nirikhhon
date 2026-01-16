package ru.otus.marchenko.changelogs;


import com.github.cloudyrock.mongock.ChangeLog;
import com.github.cloudyrock.mongock.ChangeSet;
import com.mongodb.client.MongoDatabase;
import ru.otus.marchenko.models.Author;
import ru.otus.marchenko.models.Book;
import ru.otus.marchenko.models.Comment;
import ru.otus.marchenko.models.Genre;
import ru.otus.marchenko.repositories.AuthorRepository;
import ru.otus.marchenko.repositories.BookRepository;
import ru.otus.marchenko.repositories.CommentRepository;
import ru.otus.marchenko.repositories.GenreRepository;

import java.util.List;

@ChangeLog(order = "001")
public class InitDatabaseChangelog {
    private Author author1;
    private Author author2;
    private Author author3;
    private Genre genre1;
    private Genre genre2;
    private Genre genre3;
    private Genre genre4;
    private Book book1;
    private Book book2;
    private Book book3;

    @ChangeSet(order = "000", id = "dropDB", author = "marchenko", runAlways = true)
    public void dropDB(MongoDatabase database){
        database.drop();
    }

    @ChangeSet(order = "001", id = "initAuthors", author = "marchenko", runAlways = true)
    public void initAuthors(AuthorRepository repository){
        author1 = repository.save(new Author(null, "Author_1"));
        author2 = repository.save(new Author(null, "Author_2"));
        author3 = repository.save(new Author(null, "Author_3"));
    }

    @ChangeSet(order = "001", id = "initGenres", author = "marchenko", runAlways = true)
    public void initGenres(GenreRepository repository){
        genre1 = repository.save(new Genre(null, "Genre_1"));
        genre2 = repository.save(new Genre(null, "Genre_2"));
        genre3 = repository.save(new Genre(null, "Genre_3"));
        genre4 = repository.save(new Genre(null, "Genre_4"));
    }

    @ChangeSet(order = "002", id = "initBooks", author = "marchenko", runAlways = true)
    public void initBooks(BookRepository repository){
        book1 = repository.save(new Book(null, "Book_1", author1, List.of(genre1, genre2)));
        book2 = repository.save(new Book(null, "Book_2", author2, List.of(genre2, genre4)));
        book3 = repository.save(new Book(null, "Book_3", author3, List.of(genre3, genre4)));
    }

    @ChangeSet(order = "003", id = "initComments", author = "marchenko", runAlways = true)
    public void initComments(CommentRepository repository){
        repository.save(new Comment(null, "Comment_1 by book_1", book1));
        repository.save(new Comment(null, "Comment_2 by book_1", book1));
        repository.save(new Comment(null, "Comment_3 by book_2", book2));
        repository.save(new Comment(null, "Comment_4 by book_2", book2));
        repository.save(new Comment(null, "Comment_5 by book_3", book3));
        repository.save(new Comment(null, "Comment_6 by book_3", book3));
    }
}