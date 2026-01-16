package models.library.management;

import models.library.content.Item;
import models.people.Person;

public class Comment {
    // Private Attributes ---------------------------------------------------------------
    private final Person author;
    private final  String itemId;
    private final String itemTitle;
    private final String comment;

    // Constructor ----------------------------------------------------------------------
    public Comment(Person author, Item item, String comment) {
        this.author = author;
        this.itemId = item.getId();
        this.itemTitle = item.getTitle();
        this.comment = comment;
    }

    // Getters --------------------------------------------------------------------------
    public Person getAuthor() {
        return author;
    }

    public String getItemId() {
        return itemId;
    }

    public String getItemTitle() {
        return itemTitle;
    }

    public String getComment() {
        return comment;
    }

    // Methods --------------------------------------------------------------------------

    @Override
    public String toString() {
        return "---------- Comment ----------\n" +
                "- Author: " + (this.author).getName() + (this.author).getSurname() + "\n" +
                "- Item: " + this.itemTitle +
                "- Item ID: " + this.itemId + "\n" +
                "- Comment: " + this.comment + "\n" +
                "-----------------------------";
    }
}