package com.example.spring.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.spring.dto.BookDto;
import com.example.spring.mapper.BookMapper;
import com.example.spring.models.Book;
import com.example.spring.repositories.book.BookRepository;
import com.example.spring.services.BookServiceImpl;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;

@ExtendWith(MockitoExtension.class)
public class BookServiceTest {
    @Mock
    private BookRepository bookRepository;
    @Mock
    private BookMapper bookMapper;
    @InjectMocks
    private BookServiceImpl bookService;

    @Test
    @DisplayName("Get all books from store")
    void getAll_GivenBooksInStore_ShouldReturnAllBooks() {
        // Given
        Book book1 = new Book();
        book1.setId(1L);
        book1.setTitle("Book 1");
        book1.setAuthor("Author 1");
        book1.setIsbn("1111111111");
        book1.setPrice(BigDecimal.valueOf(10.99));
        book1.setDescription("Description 1");
        book1.setCoverImage("cover_image_1.jpg");

        Book book2 = new Book();
        book2.setId(2L);
        book2.setTitle("Book 2");
        book2.setAuthor("Author 2");
        book2.setIsbn("2222222222");
        book2.setPrice(BigDecimal.valueOf(15.99));
        book2.setDescription("Description 2");
        book2.setCoverImage("cover_image_2.jpg");

        List<Book> books = Arrays.asList(book1, book2);
        List<BookDto> bookDtos = Arrays.asList(
                createBookDto(1L, "Book 1", "Author 1", "1111111111",
                        BigDecimal.valueOf(10.99), "Description 1", "cover_image_1.jpg"),
                createBookDto(2L, "Book 2", "Author 2", "2222222222",
                        BigDecimal.valueOf(15.99), "Description 2", "cover_image_2.jpg"));
        Page<Book> pageBooks = new PageImpl<>(books);
        when(bookRepository.findAll(Pageable.unpaged())).thenReturn(pageBooks);
        when(bookMapper.toDto(book1)).thenReturn(bookDtos.get(0));
        when(bookMapper.toDto(book2)).thenReturn(bookDtos.get(1));

        // When
        List<BookDto> result = bookService.findAll(Pageable.unpaged());

        // Then
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals(bookDtos.get(0).getId(), result.get(0).getId());
        assertEquals(bookDtos.get(0).getTitle(), result.get(0).getTitle());
        assertEquals(bookDtos.get(1).getId(), result.get(1).getId());
        assertEquals(bookDtos.get(1).getTitle(), result.get(1).getTitle());
        verify(bookRepository, times(1)).findAll(Pageable.unpaged());
    }

    private BookDto createBookDto(Long id, String title, String author,
                                  String isbn, BigDecimal price, String description,
                                  String coverImage) {
        BookDto bookDto = new BookDto();
        bookDto.setId(id);
        bookDto.setTitle(title);
        bookDto.setAuthor(author);
        bookDto.setIsbn(isbn);
        bookDto.setPrice(price);
        bookDto.setDescription(description);
        bookDto.setCoverImage(coverImage);
        bookDto.setCategoriesIds(null);
        return bookDto;
    }

    @Test
    @DisplayName("Should throw exception when invalid data is provided")
    void getAll_GivenInvalidBooksInStore_ShouldThrowException() {
        // Given
        Book invalidBook = new Book();
        invalidBook.setId(1L);
        invalidBook.setTitle(""); // Пустое название
        invalidBook.setAuthor("Author 1");
        invalidBook.setIsbn("1111111111");
        invalidBook.setPrice(BigDecimal.valueOf(-10.99)); // Отрицательная цена
        invalidBook.setDescription("Description 1");
        invalidBook.setCoverImage("cover_image_1.jpg");
        List<Book> invalidBooks = Arrays.asList(invalidBook);
        Page<Book> pageInvalidBooks = new PageImpl<>(invalidBooks);
        when(bookRepository.findAll(Pageable.unpaged())).thenReturn(pageInvalidBooks);
        when(bookMapper.toDto(invalidBook)).thenThrow(
                new IllegalArgumentException("Invalid book data"));

        // When & Then
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            bookService.findAll(Pageable.unpaged());
        });
        assertEquals("Invalid book data", exception.getMessage());
        verify(bookRepository, times(1)).findAll(Pageable.unpaged());
    }
}