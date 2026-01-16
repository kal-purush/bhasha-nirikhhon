package fr.umlv.javainside;

import javax.annotation.processing.SupportedAnnotationTypes;

import static java.util.Objects.requireNonNull;

public record Book(@JSONProperty("book-title") String title, int year) {
    public Book {
        requireNonNull(title);
    }
}