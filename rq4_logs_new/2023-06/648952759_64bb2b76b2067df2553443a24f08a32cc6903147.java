/*
 * Класс "Книга" (Book):

Приватные поля "название" (title) и "автор" (author) типа String для хранения названия
и автора книги соответственно.
Приватное поле "доступность" (available) типа boolean для указания доступности книги.
Публичные методы доступа (геттеры и сеттеры) для полей "название" и "автор".
Публичные методы доступа (геттеры и сеттеры) для поля "доступность".
Публичный метод "показать информацию" (displayInfo), который выводит на консоль информацию о книге
(название, автор, доступность).
 * 
 */
package lesson1.task3;

public class Book {
    private String title;
    private String author;
    private Boolean available;

    public Book(String title, String author) {
        this.title = title;
        this.author = author;
        this.available = true;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public Boolean getAvailable() {
        return available;
    }

    public void setAvailable(Boolean available) {
        this.available = available;
    }

    public String displayInfo() {
        return String.format("Название книги: %s\n"
                + "Автор: %s\n"
                + "Доступность: %s\n", this.title, this.author, this.available);
    }

    @Override
    public String toString() {
        return displayInfo();
    }
}