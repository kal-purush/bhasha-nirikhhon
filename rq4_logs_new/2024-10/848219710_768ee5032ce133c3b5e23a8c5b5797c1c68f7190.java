package com.example.spring.service;

import com.example.spring.dto.BookDto;
import com.example.spring.models.Book;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

public class BookServiceTestUtil {
    public static List<Book> getBooks() {
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

        return Arrays.asList(book1, book2);
    }

    public static List<BookDto> getBookDtos() {
        return Arrays.asList(
                createBookDto(1L, "Book 1", "Author 1", "1111111111",
                        BigDecimal.valueOf(10.99), "Description 1", "cover_image_1.jpg"),
                createBookDto(2L, "Book 2", "Author 2", "2222222222",
                        BigDecimal.valueOf(15.99), "Description 2", "cover_image_2.jpg"));
    }

    private static BookDto createBookDto(Long id, String title, String author,
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
}