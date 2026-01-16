package ru.otus.spring.service.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.otus.spring.dao.BookDao;
import ru.otus.spring.domain.Author;
import ru.otus.spring.domain.Book;
import ru.otus.spring.service.BookService;

import java.util.List;

@Service
@RequiredArgsConstructor
public class SimpleBookService implements BookService {

    private final BookDao bookDao;

    @Override
    public void addBook(Book book) {
        bookDao.addBook(book);
    }

    @Override
    public void deleteBook(Book book) {
        bookDao.deleteBook(book);
    }

    @Override
    public void editBook(Book book) {
        bookDao.updateBook(book);
    }

    @Override
    public Book getBookById(long id) {
        return bookDao.getBookById(id);
    }

    @Override
    public int countBooks() {
        return bookDao.countBooks();
    }

    @Override
    public List<Book> getBooksByAuthor(Author author) {
        return bookDao.getBooksByAuthor(author);
    }
}