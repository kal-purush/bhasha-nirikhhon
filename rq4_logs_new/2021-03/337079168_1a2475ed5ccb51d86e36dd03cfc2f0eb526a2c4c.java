package seedu.smartlib.testutil;

import seedu.smartlib.commons.core.name.Name;
import seedu.smartlib.model.book.Author;
import seedu.smartlib.model.book.Barcode;
import seedu.smartlib.model.book.Book;
import seedu.smartlib.model.book.Genre;
import seedu.smartlib.model.book.Isbn;
import seedu.smartlib.model.book.Publisher;

public class BookStub extends Book {

    /**
     * Every field must be present and not null.
     *
     * @param name Name of the book
     * @param author Author of the book
     * @param publisher Publisher of the book
     * @param isbn ISBN of the book
     * @param barcode Barcode of the book
     * @param genre Genre of the book
     */
    public BookStub(Name name, Author author, Publisher publisher, Isbn isbn, Barcode barcode, Genre genre) {
        super(name, author, publisher, isbn, barcode, genre);
    }

    /**
     * Every field must be present and not null.
     *
     * @param name Name of the book
     * @param author Author of the book
     * @param publisher Publisher of the book
     * @param isbn ISBN of the book
     * @param barcode Barcode of the book
     * @param genre Genre of the book
     * @param borrowerName Reader who borrowed the book
     */
    public BookStub(Name name, Author author, Publisher publisher, Isbn isbn, Barcode barcode, Genre genre, Name borrowerName) {
        super(name, author, publisher, isbn, barcode, genre, borrowerName);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (!(other instanceof Book)) {
            return false;
        }

        Book otherBook = (Book) other;
        return otherBook.getName().equals(getName())
                && otherBook.getAuthor().equals(getAuthor())
                && otherBook.getPublisher().equals(getPublisher())
                && otherBook.getIsbn().equals(getIsbn())
                && otherBook.getGenre().equals(getGenre());
    }

}