package com.app.bookstore.backend.service;

import com.app.bookstore.backend.DTO.BookRequestDTO;
import com.app.bookstore.backend.DTO.JsonResponseDTO;
import com.app.bookstore.backend.mapper.BookMapper;
import com.app.bookstore.backend.model.Book;
import com.app.bookstore.backend.repository.BookRepository;
import com.app.bookstore.backend.serviceimpl.BookServiceImpl;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BookServiceTest
{
    @Mock
    private BookRepository bookRepository;

    @InjectMocks
    private BookServiceImpl bookService;

    @MockBean
    private BookMapper bookMapper;

    private Book book;
    private BookRequestDTO bookRequestDTO;

    @BeforeEach
    public void init()
    {
        bookRequestDTO=BookRequestDTO.builder()
                .name("Atomic Habits")
                .price(199.09)
                .bookLogo("URL")
                .quantity(78)
                .author("James Clear")
                .description("Improving by 1% daily")
                .build();

        book=Book.builder()
                .bookId(1L)
                .bookName("Atomic Habits")
                .price(199.90)
                .bookLogo("Url")
                .author("James Clear")
                .description("Improve by 1%")
                .quantity(35)
                .build();
    }

    @Test
    public void BookService_AddBook_MustAddBook()
    {
        when(bookRepository.save(Mockito.any(Book.class))).thenReturn(book);

        JsonResponseDTO responseDTO=bookService.addBook(bookRequestDTO);

        Assertions.assertThat(responseDTO).isNotNull();
        Assertions.assertThat(responseDTO.isResult()).isEqualTo(true);
    }

    @Test
    public void BookService_GetAllBooks_MustReturnAllBooks()
    {
        when(bookRepository.findAll()).thenReturn(List.of(book));

        JsonResponseDTO responseDTO=bookService.getAllBooks();
        Assertions.assertThat(responseDTO).isNotNull();
        Assertions.assertThat(responseDTO.isResult()).isEqualTo(true);
        Assertions.assertThat(responseDTO.getData().size()).isEqualTo(1);
    }

    @Test
    public void BookService_FindById_MustReturnBookById()
    {
        when(bookRepository.findById(Mockito.any(Long.class))).thenReturn(Optional.of(book));

        JsonResponseDTO responseDTO=bookService.findById(book.getBookId());
        Assertions.assertThat(responseDTO).isNotNull();
        Assertions.assertThat(responseDTO.isResult()).isEqualTo(true);
    }

    @Test
    public void BookService_FindByName_MustReturnBookByName()
    {
        when(bookRepository.findByBookName(Mockito.any(String.class))).thenReturn(Optional.of(book));

        JsonResponseDTO responseDTO=bookService.findByName(book.getBookName());
        Assertions.assertThat(responseDTO).isNotNull();
        Assertions.assertThat(responseDTO.isResult()).isEqualTo(true);
    }

    @Test
    public void BookService_UpdateBook_MustUpdateBook()
    {
        when(bookRepository.findById(Mockito.any(Long.class))).thenReturn(Optional.of(book));
        when(bookRepository.save(Mockito.any(Book.class))).thenReturn(book);

        JsonResponseDTO responseDTO=bookService.updateBook(book.getBookId(),bookRequestDTO);
        System.out.println(responseDTO);
        Assertions.assertThat(responseDTO).isNotNull();
        Assertions.assertThat(responseDTO.isResult()).isEqualTo(true);
    }

    @Test
    public void BookService_DeleteBook_MustDeleteBook()
    {
        when(bookRepository.findById(Mockito.any(Long.class))).thenReturn(Optional.of(book));
        assertAll(()->bookService.deleteBook(book.getBookId()));
    }
}