package ru.otus.spring.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

@RequiredArgsConstructor
@Data
@Document(collection = "BOOK")
@AllArgsConstructor
public class Book {
    @Id
    private long id;

    private String title;

    private Author author;

    private Genre genre;

    @ToString.Exclude
    private List<Comment> comments;

    public Book(long id, String title, Author author, Genre genre) {
        this(id, title, author, genre, null);
    }
}