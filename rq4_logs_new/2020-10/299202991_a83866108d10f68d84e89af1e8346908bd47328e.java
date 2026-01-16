package seedu.task;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import seedu.exceptions.InvalidPriorityException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static seedu.messages.Messages.INVALID_PRIORITY;

class TaskTest {

    @Test
    void initialise_task_no_invalidPriorityExceptionThrown() {

        try {
            new Task("p1", null,
                    null, null, "1");
            new Task("p2", null,
                null, null, "2");
            new Task("p3", null,
                null, null, "3");
        } catch (InvalidPriorityException e) {
            fail();
        }
    }

    @Test
    void initialise_task_invalidPriorityExceptionThrown() {

        try {
            new Task("p0",
                null, null, null, "0");
        } catch (InvalidPriorityException e) {
            assertEquals(INVALID_PRIORITY, e.toString());
        }

        try {
            new Task("p4",
                null, null, null, "4");
        } catch (InvalidPriorityException e) {
            assertEquals(INVALID_PRIORITY, e.toString());
        }
    }


}