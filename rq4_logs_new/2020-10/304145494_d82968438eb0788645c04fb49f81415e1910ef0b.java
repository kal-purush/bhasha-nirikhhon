package wolox.training.models;

import javax.persistence.Entity;

@Entity
public class Book {

    private String Genre;
    private String Author;
    private String Image;
    private String Title;
    private String Subtitle;
    private String Publisher;
    private String Year;
    private String Pages;
    private String isbn;

    public Book(String genre, String author, String image, String title, String subtitle, String publisher, String year, String pages, String isbn) {
        Genre = genre;
        Author = author;
        Image = image;
        Title = title;
        Subtitle = subtitle;
        Publisher = publisher;
        Year = year;
        Pages = pages;
        this.isbn = isbn;
    }

    public String getGenre() {
        return Genre;
    }

    public void setGenre(String genre) {
        Genre = genre;
    }

    public String getAuthor() {
        return Author;
    }

    public void setAuthor(String author) {
        Author = author;
    }

    public String getImage() {
        return Image;
    }

    public void setImage(String image) {
        Image = image;
    }

    public String getTitle() {
        return Title;
    }

    public void setTitle(String title) {
        Title = title;
    }

    public String getSubtitle() {
        return Subtitle;
    }

    public void setSubtitle(String subtitle) {
        Subtitle = subtitle;
    }

    public String getPublisher() {
        return Publisher;
    }

    public void setPublisher(String publisher) {
        Publisher = publisher;
    }

    public String getYear() {
        return Year;
    }

    public void setYear(String year) {
        Year = year;
    }

    public String getPages() {
        return Pages;
    }

    public void setPages(String pages) {
        Pages = pages;
    }

    public String getIsbn() {
        return isbn;
    }

    public void setIsbn(String isbn) {
        this.isbn = isbn;
    }
}