package model;

import java.time.LocalDate;
import java.util.Objects;

public class Book {
    private final int id;
    private final String title;
    private final String author;
    private boolean isAvailable;
    private final int publicationYear; // Добавлено поле года издания
    private LocalDate borrowedDate; // Добавлено поле даты взятия книги


    public Book(int id, String title, String author, int publicationYear) {
        this.id = id;
        this.title = title;
        this.author = author;
        this.isAvailable = true;
        this.publicationYear = publicationYear;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Book)) return false;

        Book book = (Book) o;
        return id == book.id && isAvailable == book.isAvailable && Objects.equals(title, book.title) && Objects.equals(author, book.author);
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + Objects.hashCode(title);
        result = 31 * result + Objects.hashCode(author);
        result = 31 * result + Boolean.hashCode(isAvailable);
        return result;
    }


    @Override
    public String toString() {
        return "Книга {" +
                "ID: " + id +
                ", Название: '" + title + '\'' +
                ", Автор: '" + author + '\'' +
                ", Доступность: " + (isAvailable ? "Доступна" : "Занята") +
                '}';
    }

    public int getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public String getAuthor() {
        return author;
    }

    public int getPublicationYear() { return publicationYear; }

    public boolean isAvailable() {
        return isAvailable;
    }

    public void setAvailable(boolean available) {
        isAvailable = available;
    }

    public Object getBorrowedBy() {
        return null;
    }

    public LocalDate getBorrowedDate() { return borrowedDate; }

    public void setBorrowedDate(LocalDate borrowedDate) {
        this.borrowedDate = borrowedDate;
    }


}