package com.arjaycodes.spring5webApp.devBootstrap;

import com.arjaycodes.spring5webApp.Models.Author;
import com.arjaycodes.spring5webApp.Models.Book;

public class devBootstrap {

    private void initData() {

        // Ready Player One
        Author earnestCline = new Author("Cline", "Earnest");
        Book readyPlayerOne = new Book("Ready Player One", "978-0804190138", "Broadway Books");
        pairBookAndAuthor(readyPlayerOne, earnestCline);

        // Good Omens
        Author terryPratchett = new Author("Pratchett", "Terry");
        Author neilGaiman     = new Author("gaiman", "Neil");
        Book goodOmens        = new Book
        (
                "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch",
                "978-0060853976",
                "William Morrow Paperbacks"
        );
        pairBookAndAuthor(goodOmens, terryPratchett);
        pairBookAndAuthor(goodOmens, neilGaiman);

        // Liber Null
        Author peterCarroll = new Author("Carroll", "Peter");
        Book liberNull = new Book(
                "Liber Null & Psychonaut: An Introduction to Chaos Magick",
                "978-0877286394",
                "Weiser Books"
        );
        addBook(liberNull, peterCarroll);

        // Altered Carbon

        // Snow Crash

        // Neuromancer

        // Initiation into Hermetics

        // The Seven Laws of Influence

    }

    // Adds book to an Author's list of books
    public void addBook(Book book, Author author) {
        author.getBooks().add(book);
    }
    // Adds author to a Book's list of authors
    public void addAuthor(Book book, Author author) {
        book.getAuthors().add(author);
    }
    // Pairs book and author together
    public void pairBookAndAuthor(Book book, Author author) {
        addBook  (book, author);
        addAuthor(book, author);
    }
}