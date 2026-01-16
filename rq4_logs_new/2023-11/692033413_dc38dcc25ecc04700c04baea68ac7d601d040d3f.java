package seedu.address.model.tutorial;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TutorialTest {

    @Test
    void isTimeFormat_validInput_returnTrue() {
        assertTrue(Tutorial.isTimeFormat("Mon 1AM"));
        assertTrue(Tutorial.isTimeFormat("Mon 12AM"));
        assertTrue(Tutorial.isTimeFormat("Mon 1PM"));
        assertTrue(Tutorial.isTimeFormat("Mon 12PM"));
        assertTrue(Tutorial.isTimeFormat("Mon 1AM"));
        assertTrue(Tutorial.isTimeFormat("Tue 1AM"));
        assertTrue(Tutorial.isTimeFormat("Wed 1AM"));
        assertTrue(Tutorial.isTimeFormat("Thu 1AM"));
        assertTrue(Tutorial.isTimeFormat("Fri 1AM"));
        assertTrue(Tutorial.isTimeFormat("Sat 1AM"));
        assertTrue(Tutorial.isTimeFormat("Sun 1AM"));
    }

    @Test
    void isTimeFormat_invalidInput_returnFalse() {
        assertFalse(Tutorial.isTimeFormat("MON 1AM"));
        assertFalse(Tutorial.isTimeFormat("Monday 1AM"));
        assertFalse(Tutorial.isTimeFormat("mon 1AM"));
        assertFalse(Tutorial.isTimeFormat("Mon 0AM"));
        assertFalse(Tutorial.isTimeFormat("Mon 13AM"));
    }
}