package com.example.library.dto;

import com.example.library.entity.Author;
import com.example.library.entity.Book;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class AuthorDTO {

    private Long id;
    private String name;
    private Book book;

    public static AuthorDTO from(Author author) {
        return AuthorDTO.builder()
                .name(author.getName())
                .book(author.getBook())
                .build();
    }
}