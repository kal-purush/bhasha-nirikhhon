package seedu.smartlib.model.book;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static seedu.smartlib.logic.commands.CommandTestUtil.VALID_PUBLISHER_HARRY;
import static seedu.smartlib.logic.commands.CommandTestUtil.VALID_AUTHOR_HARRY;
import static seedu.smartlib.logic.commands.CommandTestUtil.VALID_AUTHOR_MAZE;
import static seedu.smartlib.testutil.Assert.assertThrows;
import static seedu.smartlib.testutil.TypicalModels.HARRY_PORTER;
import static seedu.smartlib.testutil.TypicalModels.MAZE;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import seedu.smartlib.model.book.exceptions.DuplicateBookException;
import seedu.smartlib.model.book.exceptions.BookNotFoundException;
import seedu.smartlib.testutil.BookBuilder;


public class UniqueBookListTest {

    private final UniqueBookList uniqueBookList = new UniqueBookList();

    @Test
    public void contains_nullBook_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> uniqueBookList.contains(null));
    }

    @Test
    public void contains_bookNotInList_returnsFalse() {
        assertFalse(uniqueBookList.contains(HARRY_PORTER));
    }

    @Test
    public void contains_bookInList_returnsTrue() {
        uniqueBookList.addBook(HARRY_PORTER);
        assertTrue(uniqueBookList.contains(HARRY_PORTER));
    }

    @Test
    public void contains_bookWithSameIdentityFieldsInList_returnsTrue() {
        uniqueBookList.addBook(HARRY_PORTER);
        Book editedBook = new BookBuilder(HARRY_PORTER).withAuthor(VALID_AUTHOR_HARRY).withPublisher(VALID_PUBLISHER_HARRY)
                .build();
        assertTrue(uniqueBookList.contains(editedBook));
    }

    @Test
    public void add_nullBook_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> uniqueBookList.addBook(null));
    }

    @Test
    public void add_duplicateBook_throwsDuplicateBookException() {
        uniqueBookList.addBook(HARRY_PORTER);
        assertThrows(DuplicateBookException.class, () -> uniqueBookList.addBook(HARRY_PORTER));
    }

    @Test
    public void setBook_nullTargetBook_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> uniqueBookList.setBook(null, HARRY_PORTER));
    }

    @Test
    public void setBook_nullEditedBook_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> uniqueBookList.setBook(HARRY_PORTER, null));
    }

    @Test
    public void setBook_targetBookNotInList_throwsBookNotFoundException() {
        assertThrows(BookNotFoundException.class, () -> uniqueBookList.setBook(HARRY_PORTER, HARRY_PORTER));
    }

    @Test
    public void setBook_editedBookIsSameBook_success() {
        uniqueBookList.addBook(HARRY_PORTER);
        uniqueBookList.setBook(HARRY_PORTER, HARRY_PORTER);
        UniqueBookList expectedUniqueBookList = new UniqueBookList();
        expectedUniqueBookList.addBook(HARRY_PORTER);
        assertEquals(expectedUniqueBookList, uniqueBookList);
    }

    @Test
    public void setBook_editedBookHasSameIdentity_success() {
        uniqueBookList.addBook(HARRY_PORTER);
        Book editedHarry = new BookBuilder(HARRY_PORTER).withAuthor(VALID_AUTHOR_MAZE).withPublisher(VALID_PUBLISHER_HARRY)
                .build();
        uniqueBookList.setBook(HARRY_PORTER, editedHarry);
        UniqueBookList expectedUniqueBookList = new UniqueBookList();
        expectedUniqueBookList.addBook(editedHarry);
        assertEquals(expectedUniqueBookList, uniqueBookList);
    }

    @Test
    public void setBook_editedBookHasDifferentIdentity_success() {
        uniqueBookList.addBook(HARRY_PORTER);
        uniqueBookList.setBook(HARRY_PORTER, MAZE);
        UniqueBookList expectedUniqueBookList = new UniqueBookList();
        expectedUniqueBookList.addBook(MAZE);
        assertEquals(expectedUniqueBookList, uniqueBookList);
    }

    @Test
    public void setBook_editedBookHasNonUniqueIdentity_throwsDuplicateBookException() {
        uniqueBookList.addBook(HARRY_PORTER);
        uniqueBookList.addBook(MAZE);
        assertThrows(DuplicateBookException.class, () -> uniqueBookList.setBook(HARRY_PORTER, MAZE));
    }

    @Test
    public void remove_nullBook_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> uniqueBookList.remove(null));
    }

    @Test
    public void remove_bookDoesNotExist_throwsBookNotFoundException() {
        assertThrows(BookNotFoundException.class, () -> uniqueBookList.remove(HARRY_PORTER));
    }

    @Test
    public void remove_existingBook_removesBook() {
        uniqueBookList.addBook(HARRY_PORTER);
        uniqueBookList.remove(HARRY_PORTER);
        UniqueBookList expectedUniqueBookList = new UniqueBookList();
        assertEquals(expectedUniqueBookList, uniqueBookList);
    }

    @Test
    public void setBooks_nullUniqueBookList_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> uniqueBookList.setBooks((UniqueBookList) null));
    }

    @Test
    public void setBooks_uniqueBookList_replacesOwnListWithProvidedUniqueBookList() {
        uniqueBookList.addBook(HARRY_PORTER);
        UniqueBookList expectedUniqueBookList = new UniqueBookList();
        expectedUniqueBookList.addBook(HARRY_PORTER);
        uniqueBookList.setBooks(expectedUniqueBookList);
        assertEquals(expectedUniqueBookList, uniqueBookList);
    }

    @Test
    public void setBooks_nullList_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> uniqueBookList.setBooks((List<Book>) null));
    }

    @Test
    public void setBooks_list_replacesOwnListWithProvidedList() {
        uniqueBookList.addBook(HARRY_PORTER);
        List<Book> bookList = Collections.singletonList(MAZE);
        uniqueBookList.setBooks(bookList);
        UniqueBookList expectedUniqueBookList = new UniqueBookList();
        expectedUniqueBookList.addBook(MAZE);
        assertEquals(expectedUniqueBookList, uniqueBookList);
    }

    @Test
    public void setBooks_listWithDuplicateBooks_throwsDuplicateBookException() {
        List<Book> listWithDuplicateBooks = Arrays.asList(HARRY_PORTER, HARRY_PORTER);
        assertThrows(DuplicateBookException.class, () -> uniqueBookList.setBooks(listWithDuplicateBooks));
    }

    @Test
    public void asUnmodifiableObservableList_modifyList_throwsUnsupportedOperationException() {
        assertThrows(UnsupportedOperationException.class, ()
                -> uniqueBookList.asUnmodifiableObservableList().remove(0));
    }

}