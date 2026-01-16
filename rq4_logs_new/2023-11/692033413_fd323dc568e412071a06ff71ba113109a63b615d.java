package seedu.address.logic.commands;

import javafx.collections.ObservableList;
import org.junit.jupiter.api.Test;
import seedu.address.commons.core.GuiSettings;
import seedu.address.logic.commands.exceptions.CommandException;
import seedu.address.model.AddressBook;
import seedu.address.model.Model;
import seedu.address.model.ReadOnlyAddressBook;
import seedu.address.model.ReadOnlyUserPrefs;
import seedu.address.model.assignment.Assignment;
import seedu.address.model.assignment.Assignment;
import seedu.address.model.module.Module;
import seedu.address.model.person.Person;
import seedu.address.model.tag.Tag;
import seedu.address.model.tutorial.Tutorial;
import seedu.address.testutil.AssignmentBuilder;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static seedu.address.testutil.Assert.assertThrows;

public class AddAssignmentCommandTest {

    @Test
    public void constructor_nullAssignment_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> new AddAssignmentCommand(null));
    }

    @Test
    public void execute_moduleAcceptedByModel_addSuccessful() throws Exception {
        ModelStubAcceptingAssignmentAdded modelStub = new ModelStubAcceptingAssignmentAdded();
        Assignment validAssignment = new AssignmentBuilder().build();

        CommandResult commandResult = new AddAssignmentCommand(validAssignment).execute(modelStub);

        assertEquals(String.format(AddAssignmentCommand.MESSAGE_SUCCESS, validAssignment), commandResult.getFeedbackToUser());
        assertEquals(Arrays.asList(validAssignment), modelStub.modulesAdded);
    }

    @Test
    public void equals() {
        Assignment moduleCS2100 = new AssignmentBuilder().build("CS2100");
        Assignment moduleCS2030 = new AssignmentBuilder().build("CS2030");

        AddAssignmentCommand addCS2100 = new AddAssignmentCommand(moduleCS2100);
        AddAssignmentCommand addCS2030 = new AddAssignmentCommand(moduleCS2030);

        // same object -> returns true
        assertTrue(addCS2100.equals(addCS2100));

        // same values -> returns true
        AddAssignmentCommand addCS2100CommandCopy = new AddAssignmentCommand(moduleCS2100);
        assertTrue(addCS2100.equals(addCS2100CommandCopy));

        // different types -> returns false
        assertFalse(addCS2100.equals(1));

        // null -> returns false
        assertFalse(addCS2100.equals(null));

        // different person -> returns false
        assertFalse(addCS2100.equals(addCS2030));
    }

    @Test
    public void execute_duplicatePerson_throwsCommandException() {
        Assignment validAssignment = new AssignmentBuilder().build();
        AddAssignmentCommand addAssignmentCommand = new AddAssignmentCommand(validAssignment);
        ModelStub modelStub = new ModelStubWithAssignment(validAssignment);

        assertThrows(CommandException.class, AddAssignmentCommand.MESSAGE_DUPLICATE_ASSIGNMENT, (
                ) -> addAssignmentCommand.execute(modelStub));
    }

    /**
     * A default model stub that have all of the methods failing.
     */
    private class ModelStub implements Model {
        @Override
        public void setUserPrefs(ReadOnlyUserPrefs userPrefs) {
            throw new AssertionError("This method should not be called.");
        }

        @Override
        public ReadOnlyUserPrefs getUserPrefs() {
            throw new AssertionError("This method should not be called.");
        }

        @Override
        public GuiSettings getGuiSettings() {
            throw new AssertionError("This method should not be called.");
        }

        @Override
        public void setGuiSettings(GuiSettings guiSettings) {
            throw new AssertionError("This method should not be called.");
        }

        @Override
        public Path getAddressBookFilePath() {
            throw new AssertionError("This method should not be called.");
        }

        @Override
        public void setAddressBookFilePath(Path addressBookFilePath) {
            throw new AssertionError("This method should not be called.");
        }

        @Override
        public void addPerson(Person person) {
            throw new AssertionError("This method should not be called.");
        }

        @Override
        public void setAddressBook(ReadOnlyAddressBook newData) {
            throw new AssertionError("This method should not be called.");
        }

        @Override
        public ReadOnlyAddressBook getAddressBook() {
            throw new AssertionError("This method should not be called.");
        }

        @Override
        public boolean hasPerson(Person person) {
            throw new AssertionError("This method should not be called.");
        }

        @Override
        public void deletePerson(Person target) {
            throw new AssertionError("This method should not be called.");
        }

        @Override
        public void setPerson(Person target, Person editedPerson) {
            throw new AssertionError("This method should not be called.");
        }

        @Override
        public ObservableList<Person> getFilteredPersonList() {
            throw new AssertionError("This method should not be called.");
        }

        @Override
        public void updateFilteredPersonList(Predicate<Person> predicate) {
            throw new AssertionError("This method should not be called.");
        }

        @Override
        public void addModule(Module module) {
            throw new AssertionError("This method should not be called.");
        }

        @Override
        public void deleteModule(Module module) {
            throw new AssertionError("This method should not be called.");
        }

        @Override
        public void addTutorial(Tutorial module) {
            throw new AssertionError("This method should not be called.");
        }

        @Override
        public void deleteTutorial(Tutorial tutorial) {
            throw new AssertionError("This method should not be called.");
        }

        @Override
        public boolean hasModule(Module module) {
            throw new AssertionError("This method should not be called.");
        }

        @Override
        public boolean hasTutorial(Tutorial tutorial) {
            throw new AssertionError("This method should not be called.");
        }

        @Override
        public ObservableList<Module> getModuleList() {
            throw new AssertionError("This method should not be called.");
        }

        @Override
        public ObservableList<Tutorial> getTutorialList() {
            throw new AssertionError("This method should not be called");
        }

        @Override
        public void addAssignment(Assignment assignment) {
            throw new AssertionError("This method should not be called");
        }

        @Override
        public void deleteAssignment(Assignment assignment) {

        }

        @Override
        public boolean hasAssignment(Assignment assignment) {
            throw new AssertionError("This method should not be called");
        }

        @Override
        public ObservableList<Assignment> getAssignmentList() {
            throw new AssertionError("This method should not be called");
        }

        @Override
        public void addAttendanceTag(Tag tag) {
            throw new AssertionError("This method should not be called.");
        }

        @Override
        public void deleteAttendanceTag(Tag tag) {
            throw new AssertionError("This method should not be called.");
        }

        @Override
        public boolean hasAttendanceTag(Tag tag) {
            throw new AssertionError("This method should not be called.");
        }

        @Override
        public List<Tag> getAttendanceTagsList() {
            throw new AssertionError("This method should not be called.");
        }
    }

    /**
     * A Model stub that contains a single module.
     */
    private class ModelStubWithAssignment extends ModelStub {
        private final Assignment module;

        ModelStubWithAssignment(Assignment module) {
            requireNonNull(module);
            this.module = module;
        }

        @Override
        public boolean hasAssignment(Assignment person) {
            requireNonNull(person);
            return this.module.isSameAssignment(person);
        }
    }

    /**
     * A Model stub that always accept the module being added.
     */
    private class ModelStubAcceptingAssignmentAdded extends ModelStub {
        final ArrayList<Assignment> modulesAdded = new ArrayList<>();

        @Override
        public boolean hasAssignment(Assignment person) {
            requireNonNull(person);
            return modulesAdded.stream().anyMatch(person::isSameAssignment);
        }

        @Override
        public void addAssignment(Assignment module) {
            requireNonNull(module);
            modulesAdded.add(module);
        }

        @Override
        public ReadOnlyAddressBook getAddressBook() {
            return new AddressBook();
        }
    }
}