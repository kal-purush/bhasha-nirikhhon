package seedu.commands;

import org.junit.jupiter.api.Test;
import seedu.data.TaskMap;
import seedu.exceptions.InvalidCommandException;
import seedu.exceptions.InvalidDatetimeException;
import seedu.exceptions.InvalidPriorityException;
import seedu.exceptions.MaxNumTaskException;
import seedu.task.Task;
import static seedu.messages.Messages.ADD_MESSAGE;

import static org.junit.jupiter.api.Assertions.*;

class AddTest {

    @Test
    public void constructor_noRawInput_throws_InvalidCommandException() {
        assertThrows(InvalidCommandException.class, () -> new Add(""));
    }

    @Test
    public void addCommand_wrongStartTimeFormat(){
        final String[] invalidInputs = { "", " ", "1231242345", "&*^%*&^", "abc"};
        for(int i=0; i< invalidInputs.length; i++) {
            int finalI = i;
            assertThrows(InvalidCommandException.class, () -> new Add("add task1 st/" + invalidInputs[finalI]));
        }
    }

    @Test
    public void addCommand_wrongEndTimeFormat(){
        final String[] invalidInputs = { "", " ", "1231242345", "&*^%*&^", "abc"};
        for(int i=0; i< invalidInputs.length; i++) {
            int finalI = i;
            assertThrows(InvalidCommandException.class, () -> new Add("add task1 et/" + invalidInputs[finalI]));
        }
    }

    @Test
    public void addCommand_wrongDateFormat(){
        final String[] invalidInputs = { "", " ", "1231242345", "&*^%*&^", "abc"};
        for(int i=0; i< invalidInputs.length; i++) {
            int finalI = i;
            assertThrows(InvalidCommandException.class, () -> new Add("add task1 d/" + invalidInputs[finalI]));
        }
    }

    @Test
    public void addCommand_wrongPriorityFormat(){
        final String[] invalidInputs = { "", " ", "1231242345", "&*^%*&^", "abc"};
        for(int i=0; i< invalidInputs.length; i++) {
            int finalI = i;
            assertThrows(InvalidCommandException.class, () -> new Add("add task1 p/" + invalidInputs[finalI]));
        }
    }

}