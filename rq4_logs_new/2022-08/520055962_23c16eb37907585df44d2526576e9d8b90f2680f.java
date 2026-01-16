package ru.netology.homework12;

public class Book extends Product {

    private String nameBook;
    private String author;

    public Book(int id, String name, int cost, String nameBook, String author) {
        super(id, name, cost);
        this.nameBook = nameBook;
        this.author = author;
    }

    public String getNameBook() {
        return nameBook;
    }

    public void setNameBook(String nameBook) {
        this.nameBook = nameBook;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }
}