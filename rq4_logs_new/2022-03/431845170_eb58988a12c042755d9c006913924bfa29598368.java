package creational_pattern.factoryMethod._me_02;

import java.time.LocalDateTime;

public class Book {
    private Long id;
    private String name;
    private String isbn;
    private LocalDateTime publicationTime;

    public Book() {
    }

    public Book(Long id, String name, String isbn, LocalDateTime publicationTime) {
        this.id = id;
        this.name = name;
        this.isbn = isbn;
        this.publicationTime = publicationTime;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getIsbn() {
        return isbn;
    }

    public void setIsbn(String isbn) {
        this.isbn = isbn;
    }

    public LocalDateTime getPublicationTime() {
        return publicationTime;
    }

    public void setPublicationTime(LocalDateTime publicationTime) {
        this.publicationTime = publicationTime;
    }
}