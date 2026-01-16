package es.uah.matcomp.mp.e1.composagre.b;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BookTest {

    @Test
    void getName() {
        Author ahTeck = new Author("Tan Ah Teck", "ahteck@nowhere.com", 'm');
        Book dummyBook = new Book("Java for dummy", ahTeck, 19.95);
        assertEquals("Java for dummy", dummyBook.getName());
    }

    @Test
    void getAuthor() {
        Author ahTeck = new Author("Tan Ah Teck", "ahteck@nowhere.com", 'm');
        Book dummyBook = new Book("Java for dummy", ahTeck, 19.95, 99);
        assertEquals(ahTeck, dummyBook.getAuthor());
    }

    @Test
    void getPrice() {
        Author ahTeck = new Author("Tan Ah Teck", "ahteck@nowhere.com", 'm');
        Book dummyBook = new Book("Java for dummy", ahTeck, 19.95, 99);
        assertEquals(19.95, dummyBook.getPrice(), 0);
    }

    @Test
    void setPrice() {
        Author ahTeck = new Author("Tan Ah Teck", "ahteck@nowhere.com", 'm');
        Book dummyBook = new Book("Java for dummy", ahTeck, 19.95, 99);
        dummyBook.setPrice(25);
        assertEquals(25, dummyBook.getPrice());

    }

    @Test
    void getQty() {
        Author ahTeck = new Author("Tan Ah Teck", "ahteck@nowhere.com", 'm');
        Book dummyBook = new Book("Java for dummy", ahTeck, 19.95, 99);
        assertEquals(99, dummyBook.getQty());
    }

    @Test
    void setQty() {
        Author ahTeck = new Author("Tan Ah Teck", "ahteck@nowhere.com", 'm');
        Book dummyBook = new Book("Java for dummy", ahTeck, 19.95, 99);
        dummyBook.setQty(150);
        assertEquals(150, dummyBook.getQty());
    }

    @Test
    void testToString() {
        Author ahTeck = new Author("Tan Ah Teck", "ahteck@nowhere.com", 'm');
        Book dummyBook = new Book("Java for dummy", ahTeck, 19.95, 99);
        assertEquals("Book [name=Java for dummy,Author[name=Tan Ah Teck, email=ahteck@nowhere.com, gender=m], price=19.95, qty=99]", dummyBook.toString());
    }

    @Test
    void getAuthorName() {
        Author ahTeck = new Author("Tan Ah Teck", "ahteck@nowhere.com", 'm');
        Book dummyBook = new Book("Java for dummy", ahTeck, 19.95, 99);
        assertEquals("Tan Ah Teck", dummyBook.getAuthorName());
    }

    @Test
    void getAuthorEmail() {
        Author ahTeck = new Author("Tan Ah Teck", "ahteck@nowhere.com", 'm');
        Book dummyBook = new Book("Java for dummy", ahTeck, 19.95, 99);
        assertEquals("ahteck@nowhere.com", dummyBook.getAuthorEmail());
    }

    @Test
    void getAuthorGender() {
        Author ahTeck = new Author("Tan Ah Teck", "ahteck@nowhere.com", 'm');
        Book dummyBook = new Book("Java for dummy", ahTeck, 19.95, 99);
        assertEquals('m', dummyBook.getAuthorGender());
    }
}