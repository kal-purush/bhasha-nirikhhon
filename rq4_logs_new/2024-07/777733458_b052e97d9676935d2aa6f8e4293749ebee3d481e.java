package com.practice.practiceProject.dto;

import com.practice.practiceProject.constant.BookConstant;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class BookDto {

    @Size(min = 5, message = BookConstant.SHORT_BOOK_TITLE)
    @Size(max = 20, message = BookConstant.LONG_BOOK_TITLE)
    private String title;

    @Size(min = 5,message = BookConstant.SHORT_AUTHOR_NAME)
    @Size(max = 20, message = BookConstant.LONG_AUTHOR_NAME)
    private String authorName;

    private String isbn;  //Unique identifier for book

    @NotNull(message = BookConstant.NULL_BOOK_PRICE)
    private Double price;

    @NotNull(message = BookConstant.NULL_RENTAL_PRICE)
    private Double rentalPrice;

    @NotNull(message = BookConstant.NULL_BOOK_STOCK)
    private Integer stock;

    public void setTitle(String title) {
        this.title = title != null ? title.trim() : null;
    }

    public void setAuthorName(String authorName) {
        this.authorName = authorName != null ? authorName.trim() : null;
    }

    public void setIsbn(String isbn) {
        this.isbn = isbn != null ? isbn.trim() : null;
    }
}