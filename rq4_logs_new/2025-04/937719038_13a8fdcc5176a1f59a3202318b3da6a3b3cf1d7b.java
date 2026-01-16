package seedu.tassist.logic.commands;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static seedu.tassist.logic.commands.CommandTestUtil.assertCommandFailure;
import static seedu.tassist.logic.commands.CommandTestUtil.assertCommandSuccess;
import static seedu.tassist.logic.commands.CommandTestUtil.showPersonAtIndex;
import static seedu.tassist.logic.commands.TagCommand.MESSAGE_DEL_INVALID_TAGS;
import static seedu.tassist.logic.commands.TagCommand.MESSAGE_DEL_NO_MORE_ITEMS;
import static seedu.tassist.logic.commands.TagCommand.MESSAGE_DUPLICATE_TAG;
import static seedu.tassist.logic.commands.TagCommand.MESSAGE_EDIT_EXISTENT_NEW_TAG;
import static seedu.tassist.logic.commands.TagCommand.MESSAGE_EDIT_OLD_TAG_NOT_FOUND;
import static seedu.tassist.logic.commands.TagCommand.MESSAGE_TAG_SUCCESS;
import static seedu.tassist.logic.commands.TagCommand.getTagSummary;
import static seedu.tassist.testutil.TypicalIndexes.INDEX_FIRST_PERSON;
import static seedu.tassist.testutil.TypicalIndexes.INDEX_SECOND_PERSON;
import static seedu.tassist.testutil.TypicalIndexes.INDEX_THIRD_PERSON;
import static seedu.tassist.testutil.TypicalPersons.getTypicalAddressBook;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import seedu.tassist.commons.core.index.Index;
import seedu.tassist.logic.Messages;
import seedu.tassist.logic.parser.TagCommandParser;
import seedu.tassist.model.AddressBook;
import seedu.tassist.model.Model;
import seedu.tassist.model.ModelManager;
import seedu.tassist.model.UserPrefs;
import seedu.tassist.model.person.Person;
import seedu.tassist.model.tag.Tag;
import seedu.tassist.testutil.PersonBuilder;

public class TagCommandTest {

    private Model model = new ModelManager(getTypicalAddressBook(), new UserPrefs());
    private HashSet<Tag> tags = new HashSet<>(List.of(
            new Tag("testTag1"),
            new Tag("testTag2")
    ));



    // ================= JUnits for Tags (General) =================
    @Test
    public void constructor_nullParams_throwsNullPointerException() {
        // Index is null
        assertThrows(NullPointerException.class, () ->
                new TagCommand(null, TagCommandParser.ActionType.ADD, tags, null, null));
        // Action Type is null
        assertThrows(NullPointerException.class, () ->
                new TagCommand(INDEX_FIRST_PERSON, null, tags, null, null));

        // Set of tags are null when action type is ADD or DEL
        assertThrows(NullPointerException.class, () ->
                new TagCommand(INDEX_FIRST_PERSON, TagCommandParser.ActionType.ADD, null, null, null));

        assertThrows(NullPointerException.class, () ->
                new TagCommand(INDEX_FIRST_PERSON, TagCommandParser.ActionType.DEL, null, null, null));
        // oldTag and newTag are null when action type is EDIT
        assertThrows(NullPointerException.class, () ->
                new TagCommand(INDEX_FIRST_PERSON, TagCommandParser.ActionType.EDIT, null, null, null));
    }

    @Test
    public void execute_invalidPersonIndexUnfilteredList_failure() {
        Index index = Index.fromOneBased(model.getFilteredPersonList().size() + 1);
        TagCommand tagCommand = new TagCommand(index, TagCommandParser.ActionType.ADD, tags, null, null);
        assertCommandFailure(tagCommand, model,
                String.format(Messages.MESSAGE_INVALID_PERSON_DISPLAYED_INDEX,
                        model.getFilteredPersonList().size()));
    }

    @Test
    public void execute_invalidPersonIndexFilteredList_failure() {
        showPersonAtIndex(model, INDEX_FIRST_PERSON);
        Index outOfBoundIndex = INDEX_SECOND_PERSON;
        // Ensures that outOfBoundIndex is still in bounds of address book list
        assertTrue(outOfBoundIndex.getZeroBased() < model.getAddressBook().getPersonList().size());

        TagCommand tagCommand = new TagCommand(outOfBoundIndex, TagCommandParser.ActionType.ADD, tags, null, null);
        assertCommandFailure(tagCommand, model,
                String.format(Messages.MESSAGE_INVALID_PERSON_DISPLAYED_INDEX,
                        model.getFilteredPersonList().size()));
    }

    // ================= JUnits for Tags (Add) =================
    private static String[] convertSetToArray(Set<Tag> set) {
        return set.stream()
                .map(tag -> tag.tagName)
                .toArray(String[]::new);
    }

    @Test
    public void execute_allFieldsSpecifiedAddTag_success() {
        //Removing just to test out single tag
        tags.remove(new Tag("testTag2"));
        HashSet<Tag> newTags = new HashSet<>(tags);
        newTags.addAll(model.getFilteredPersonList().get(0).getTags());
        Person editedPerson = new PersonBuilder(model.getFilteredPersonList().get(0))
                .withTags(convertSetToArray(newTags)).build();
        TagCommand tagCommand = new TagCommand(INDEX_FIRST_PERSON, TagCommandParser.ActionType.ADD, tags, null, null);
        String expectedMessage = String.format(MESSAGE_TAG_SUCCESS,
                TagCommandParser.ActionType.ADD, getTagSummary(editedPerson));
        Model expectedModel = new ModelManager(new AddressBook(model.getAddressBook()), new UserPrefs());
        expectedModel.setPerson(model.getFilteredPersonList().get(0), editedPerson);
        assertCommandSuccess(tagCommand, model, expectedMessage, expectedModel);

        // Adding it back for multi
        tags.add(new Tag("testTag2"));
        newTags = new HashSet<>(tags);
        newTags.addAll(model.getFilteredPersonList().get(0).getTags());
        editedPerson = new PersonBuilder(model.getFilteredPersonList().get(0))
                .withTags(convertSetToArray(newTags)).build();
        tagCommand = new TagCommand(INDEX_FIRST_PERSON, TagCommandParser.ActionType.ADD, tags, null, null);
        expectedMessage = String.format(MESSAGE_TAG_SUCCESS,
                TagCommandParser.ActionType.ADD, getTagSummary(editedPerson));
        expectedModel = new ModelManager(new AddressBook(model.getAddressBook()), new UserPrefs());
        expectedModel.setPerson(model.getFilteredPersonList().get(0), editedPerson);
        assertCommandSuccess(tagCommand, model, expectedMessage, expectedModel);
    }

    @Test
    public void execute_addDuplicatedTags_failure() {
        Set<Tag> exisitingTags = model.getFilteredPersonList().get(0).getTags();
        TagCommand tagCommand = new TagCommand(INDEX_FIRST_PERSON, TagCommandParser.ActionType.ADD,
                exisitingTags, null, null);
        assertCommandFailure(tagCommand, model, MESSAGE_DUPLICATE_TAG);
    }

    // ================= JUnits for Tags (Delete) =================
    @Test
    public void execute_allFieldsSpecifiedDelTag_success() {
        // Deleting a single tag
        //Removing just to test out single tag
        tags.remove(new Tag("testTag2"));
        HashSet<Tag> newTags = new HashSet<>(tags);
        newTags.addAll(model.getFilteredPersonList().get(0).getTags());
        Person editedPerson = new PersonBuilder(model.getFilteredPersonList().get(0))
                .withTags(convertSetToArray(newTags)).build();
        TagCommand tagCommand = new TagCommand(INDEX_FIRST_PERSON, TagCommandParser.ActionType.DEL, tags, null, null);
        String expectedMessage = String.format(MESSAGE_TAG_SUCCESS,
                TagCommandParser.ActionType.DEL, getTagSummary(model.getFilteredPersonList().get(0)));
        Model expectedModel = new ModelManager(new AddressBook(model.getAddressBook()), new UserPrefs());
        model.setPerson(model.getFilteredPersonList().get(0), editedPerson);
        assertCommandSuccess(tagCommand, model, expectedMessage, expectedModel);

        // Adding it back for multi
        tags.add(new Tag("testTag2"));
        newTags = new HashSet<>(tags);
        newTags.addAll(model.getFilteredPersonList().get(0).getTags());
        editedPerson = new PersonBuilder(model.getFilteredPersonList().get(0))
                .withTags(convertSetToArray(newTags)).build();
        tagCommand = new TagCommand(INDEX_FIRST_PERSON, TagCommandParser.ActionType.DEL, tags, null, null);
        expectedMessage = String.format(MESSAGE_TAG_SUCCESS,
                TagCommandParser.ActionType.DEL, getTagSummary(model.getFilteredPersonList().get(0)));
        expectedModel = new ModelManager(new AddressBook(model.getAddressBook()), new UserPrefs());
        model.setPerson(model.getFilteredPersonList().get(0), editedPerson);
        assertCommandSuccess(tagCommand, model, expectedMessage, expectedModel);
    }

    @Test
    public void execute_deleteTagsWhenNoTags_failure() {
        Set<Tag> existingTags = model.getFilteredPersonList().get(1).getTags();
        TagCommand tagCommand = new TagCommand(INDEX_THIRD_PERSON, TagCommandParser.ActionType.DEL,
                existingTags, null, null);
        assertCommandFailure(tagCommand, model, MESSAGE_DEL_NO_MORE_ITEMS);
    }

    @Test
    public void execute_deleteNonExistentTags_failure() {
        TagCommand tagCommand = new TagCommand(INDEX_FIRST_PERSON, TagCommandParser.ActionType.DEL,
                tags, null, null);
        assertCommandFailure(tagCommand, model, MESSAGE_DEL_INVALID_TAGS);
    }

    // ================= JUnits for Tags (Edit) =================
    @Test
    public void execute_allFieldsSpecifiedEditTag_success() {
        HashSet<Tag> newTags = new HashSet<>(model.getFilteredPersonList().get(0).getTags());
        String[] stringTags = convertSetToArray(newTags);
        TagCommand tagCommand = new TagCommand(INDEX_FIRST_PERSON, TagCommandParser.ActionType.EDIT,
                null, new Tag(stringTags[0]), new Tag("testTag2"));
        Person editedPerson = new PersonBuilder(model.getFilteredPersonList().get(0))
                .withTags("testTag2").build();
        String expectedMessage = String.format(MESSAGE_TAG_SUCCESS,
                TagCommandParser.ActionType.EDIT, getTagSummary(editedPerson));
        Model expectedModel = new ModelManager(new AddressBook(model.getAddressBook()), new UserPrefs());
        expectedModel.setPerson(model.getFilteredPersonList().get(0), editedPerson);
        assertCommandSuccess(tagCommand, model, expectedMessage, expectedModel);
    }

    @Test
    public void execute_editNonExistentOldTag_failure() {
        TagCommand tagCommand = new TagCommand(INDEX_FIRST_PERSON, TagCommandParser.ActionType.EDIT,
                null, new Tag("testTag1"), new Tag("testTag2"));
        assertCommandFailure(tagCommand, model, MESSAGE_EDIT_OLD_TAG_NOT_FOUND);
    }

    @Test
    public void execute_editExistentNewTag_failure() {
        String[] tagArray = convertSetToArray(model.getFilteredPersonList().get(1).getTags());
        TagCommand tagCommand = new TagCommand(INDEX_SECOND_PERSON, TagCommandParser.ActionType.EDIT,
                null, new Tag(tagArray[0]), new Tag(tagArray[1]));
        assertCommandFailure(tagCommand, model, MESSAGE_EDIT_EXISTENT_NEW_TAG);
    }
}