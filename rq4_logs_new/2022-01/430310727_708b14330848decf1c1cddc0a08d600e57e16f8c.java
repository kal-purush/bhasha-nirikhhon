package ru.otus.spring.service;

import ru.otus.spring.domain.Author;
import ru.otus.spring.domain.Book;

import java.util.List;

public interface BookService {
    public void addBook(String title,long authorId,long genreId);

    void deleteBook(Book book);

    void saveBook(Book book);

    Book getBookById(long id);

    long countBooks();

//    boolean addBookComment(long id, String content);

//    boolean deleteBookComment(long commentId);

    List<Book> getBooksByAuthor(Author author);

}