package seedu.smartlib.logic.commands;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static seedu.smartlib.testutil.Assert.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import seedu.smartlib.logic.commands.exceptions.CommandException;
import seedu.smartlib.model.ReadOnlySmartLib;
import seedu.smartlib.model.SmartLib;
import seedu.smartlib.model.book.Book;
import seedu.smartlib.testutil.BookBuilder;

public class AddBookCommandTest {

    @Test
    public void constructor_nullBook_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> new AddBookCommand(null));
    }

    @Test
    public void execute_bookAcceptedByModel_addSuccessful() throws Exception {
        ModelStubAcceptingBookAdded modelStub = new ModelStubAcceptingBookAdded();
        Book validBook = new BookBuilder().build();

        CommandResult commandResult = new AddBookCommand(validBook).execute(modelStub);

        assertEquals(String.format(AddBookCommand.MESSAGE_SUCCESS, validBook), commandResult.getFeedbackToUser());
        assertEquals(Arrays.asList(validBook), modelStub.booksAdded);
    }

    @Test
    public void execute_duplicateBook_throwsCommandException() {
        Book validBook = new BookBuilder().build();
        AddBookCommand addBookCommand = new AddBookCommand(validBook);
        ModelStub modelStub = new ModelStubWithBook(validBook);

        assertThrows(CommandException.class, AddBookCommand.MESSAGE_DUPLICATE_BOOK, () ->
                addBookCommand.execute(modelStub));
    }

    @Test
    public void equals() {
        Book harry = new BookBuilder().withName("Harry Potter").build();
        Book land = new BookBuilder().withName("A Promise Land").build();
        AddBookCommand addHarryCommand = new AddBookCommand(harry);
        AddBookCommand addLandCommand = new AddBookCommand(land);

        // same object -> returns true
        assertTrue(addHarryCommand.equals(addHarryCommand));

        // same values -> returns true
        AddBookCommand addHarryCommandCopy = new AddBookCommand(harry);
        assertTrue(addHarryCommand.equals(addHarryCommandCopy));

        // different types -> returns false
        assertFalse(addHarryCommand.equals(1));

        // null -> returns false
        assertFalse(addHarryCommand.equals(null));

        // different reader -> returns false
        assertFalse(addHarryCommand.equals(addLandCommand));
    }


    /**
     * A Model stub that contains a single book.
     */
    private class ModelStubWithBook extends ModelStub {
        private final Book book;

        ModelStubWithBook(Book book) {
            requireNonNull(book);
            this.book = book;
        }

        @Override
        public boolean hasBook(Book book) {
            requireNonNull(book);
            return this.book.isSameBook(book);
        }
    }

    /**
     * A Model stub that always accept the book being added.
     */
    private class ModelStubAcceptingBookAdded extends ModelStub {
        final ArrayList<Book> booksAdded = new ArrayList<>();

        @Override
        public boolean hasBook(Book book) {
            requireNonNull(book);
            return booksAdded.stream().anyMatch(book::isSameBook);
        }

        @Override
        public void addBook(Book book) {
            requireNonNull(book);
            booksAdded.add(book);
        }

        @Override
        public ReadOnlySmartLib getSmartLib() {
            return new SmartLib();
        }
    }

}
