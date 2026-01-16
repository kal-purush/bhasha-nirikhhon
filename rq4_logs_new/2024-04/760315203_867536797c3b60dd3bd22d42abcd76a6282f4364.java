package seedu.address.logic.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static seedu.address.testutil.Assert.assertThrows;

import org.junit.jupiter.api.Test;

import seedu.address.model.person.Name;
import seedu.address.model.person.Person;
import seedu.address.model.project.Member;
import seedu.address.testutil.PersonBuilder;



class AddCommentCommandTest {
    private Person taskProject = new PersonBuilder().build();
    private Member member = new Member("James");

    @Test
    public void constructor_nullPerson_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> new AddCommentCommand(null, member, "wait"));
        assertThrows(NullPointerException.class, () -> new AddCommentCommand(taskProject, null, "wait"));
        assertThrows(NullPointerException.class, () -> new AddCommentCommand(taskProject, member, null));

    }

    @Test
    public void equals() {
        Member memberBob = new Member("Bob");
        Person projectB = new Person(new Name("B"));
        AddCommentCommand addAComment = new AddCommentCommand(taskProject, member, "A");
        AddCommentCommand addBComment = new AddCommentCommand(taskProject, member, "B");
        AddCommentCommand addACommentDiffMember = new AddCommentCommand(taskProject, memberBob, "A");
        AddCommentCommand addACommentDiffProject = new AddCommentCommand(projectB, member, "A");


        // same object -> returns true
        assertTrue(addAComment.equals(addAComment));

        // same values -> returns true
        AddCommentCommand addACommentCopy = new AddCommentCommand(taskProject, member, "A");
        assertTrue(addAComment.equals(addACommentCopy));

        // different types -> returns false
        assertFalse(addAComment.equals(1));

        // null -> returns false
        assertFalse(addAComment.equals(null));

        // different comment -> returns false
        assertFalse(addAComment.equals(addBComment));
        assertFalse(addAComment.equals(addACommentDiffMember));
        assertFalse(addAComment.equals(addACommentDiffProject));

    }
    @Test
    public void toStringMethod() {
        AddCommentCommand addCommentCommand = new AddCommentCommand(taskProject, member, "A");
        String expected = AddCommentCommand.class.getCanonicalName() + "{toAddTo=" + taskProject + ", "
                + "toAdd=A, " + "toAddFrom=" + member + "}";
        assertEquals(expected, addCommentCommand.toString());
    }
}