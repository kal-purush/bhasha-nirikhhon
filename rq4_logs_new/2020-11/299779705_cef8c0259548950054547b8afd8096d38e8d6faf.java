package com.example.cmput301f20t18;

import org.testng.annotations.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Chase-Warwick
 */
public class BookTest {
    private String defaultTitle = "TEST_TITLE";
    private Long defaultISBN = 12345678910L;
    private String defaultAuthor = "TEST_AUTHOR";
    private int defaultID = 0;
    private int defaultStatus = Book.STATUS_AVAILABLE;
    private int defaultYear = 1999;
    private String defaultOwnerDbID = "TEST_DB_OWNER_ID";
    private String defaultOwnerUsername = "TEST_OWNER";
    private ArrayList<String> defaultPhotos = new ArrayList<String>();

    private Book mockBook(){
        return new Book(defaultTitle, defaultISBN, defaultAuthor, defaultID, defaultStatus,
                defaultYear, defaultOwnerDbID, defaultOwnerUsername, defaultPhotos);
    }
    @Test
    void testGetTitle(){
        Book book = mockBook();
        assertTrue(book.getTitle().equals(defaultTitle));
    }
    @Test
    void testSetTitle(){
        Book book = mockBook();
        String title = "Hello World";
        book.setTitle(title);
        assertTrue(book.getTitle().equals(title));
    }
    @Test
    void testGetAuthor(){
        Book book = mockBook();
        assertTrue(book.getAuthor().equals(defaultAuthor));
    }
    @Test
    void testSetAuthor(){
        Book book = mockBook();
        String author = "Hello World";
        book.setAuthor(author);
        assertTrue(book.getTitle().equals(author));
    }
    @Test
    void testGetId(){
        Book book = mockBook();
        assertEquals(defaultID, book.getId());
    }
    @Test
    void testSetId(){
        Book book = mockBook();
        int ID = -1;
        book.setId(ID);
        assertEquals(ID, book.getId());
    }
    @Test
    void testGetStatus(){
        Book book = mockBook();
        assertEquals(defaultStatus, book.getStatus());
    }
    @Test
    void testSetStatus(){
        Book book = mockBook();
        int status = Book.STATUS_REQUESTED;
        book.setStatus(status);
        assertEquals(status, book.getStatus());
    }
    @Test
    void testGetYear(){
        Book book = mockBook();
        assertEquals(defaultYear, book.getYear());
    }
    @Test
    void testSetYear(){
        Book book = mockBook();
        int year = 2020;
        book.setYear(year);
        assertEquals(year, book.getYear());
    }
    @Test
    void testGetISBN(){
        Book book = mockBook();
        assertEquals((long)defaultISBN, (long)book.getISBN());
    }
    @Test
    void testSetISBN(){
        Book book = mockBook();
        Long isbn = 10987654321L;
        book.setIsbn(isbn);
        assertEquals((long)isbn, (long)book.getISBN());
    }
    @Test
    void testGetPhotos(){
        Book book = mockBook();
        ArrayList<String> photos = book.getPhotos();
        assertTrue(photos.isEmpty());
    }
    @Test
    void testSetPhotos(){
        Book book = mockBook();
        ArrayList<String> photos = book.getPhotos();
        assertTrue(photos.isEmpty());
        String photo = "SAMPLE_PHOTO";
        photos.add(photo);
        book.setPhotos(photos);

        photos = book.getPhotos();
        assertFalse(photos.isEmpty());
        assertTrue(photo.equals(photos.get(0)));
    }
    @Test
    void testSetBorrower_UsernameAndGetBorrowerUsername(){
        Book book = mockBook();
        String borrowerUsername = "BORROWER_USERNAME";
        book.setBorrower_username(borrowerUsername);
        assertTrue(book.getBorrower_username().equals(borrowerUsername));
    }
    @Test
    void testGetOwner_Username(){
        Book book = mockBook();
        assertTrue(book.getOwner_username().equals(defaultOwnerUsername));
    }
    @Test
    void testGetOwner_dbID(){
        Book book = mockBook();
        assertTrue(book.getOwner_dbID().equals(defaultOwnerDbID));
    }
    @Test
    void testHasPhoto(){
        Book book = mockBook();
        assertFalse(book.hasPhotos());
        ArrayList<String> photos = new ArrayList<String>();
        photos.add("Sample_Photo");
        book.setPhotos(photos);
        assertTrue(book.hasPhotos());
    }
    @Test
    void retrieveCover(){
        Book book = mockBook();
        assertEquals(null, book.retrieveCover());
    }
    @Test
    void retrievePhotos(){
        Book book = mockBook();
        assertTrue(book.retrievePhotos().isEmpty());
    }
}