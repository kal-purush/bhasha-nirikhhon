package com.example.backend.model.dto;

import com.example.backend.model.Author;
import com.example.backend.model.enums.BookCategory;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
public class BookDto {
    private String name;

    private BookCategory category;

    private Long author;

    private int availableCopies;

    public BookDto(String name, BookCategory category, Long author, int availableCopies) {
        this.name = name;
        this.category = category;
        this.author = author;
        this.availableCopies = availableCopies;
    }
}