package model;


import model.Subtask;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SubtaskTest {



    @Test
    public void equals_returnTrue_passedIdsMatch() {
        Subtask subtask1 = new Subtask("name", "description", 1);
        Subtask subtask2 = new Subtask("name", "description", 1);
        assertTrue(subtask1.equals(subtask2));
    }

    @Test

    public void addIdSubtask_dontAdd_addToYourEpic() { // Нельзя Subtask сделать своим Эпик
        Subtask subtask = new Subtask("name", "description", 1);

        subtask.setId(2); // устанавливает первоначальный id
        assertEquals(2,subtask.getId()); // убеждаемся, что id предводился

        subtask.setId(1); // устанавливает id равный Эпику
        assertNotEquals(1,subtask.getId());

        subtask.setEpicId(2); // устанавливает привязку к Эпику равной своему id
        assertNotEquals(2,subtask.getEpicId());
    }
}