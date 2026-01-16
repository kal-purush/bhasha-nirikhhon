package ics372Prgm1Items;

import java.util.Calendar;
/**
 * Created by Kevin on 1/22/2017.
 */
public class Book extends Item {

    private String author;

    public Book() {
        super();
    }

    public Book(String id, String name, String type, String author) {
        super(id, name, type);
        this.author = author;
        checkOutTime = 21;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

}