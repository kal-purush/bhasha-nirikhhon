package cooper.meetings;

import cooper.exceptions.DuplicateUsernameException;
import cooper.exceptions.InvalidTimeException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Array;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MeetingManagerTest {
    static MeetingManager meetingManager;

    @BeforeAll
    static void setUpMeetingManager() {
        meetingManager = new MeetingManager();
    }

    @Test
    @Order(1)
    void addAvailability_invalidTimeFormat_expectException() {
        String inputTime = "12.53";
        String inputName = "shixi";
        assertThrows(InvalidTimeException.class,
                () -> meetingManager.addAvailability(inputTime, inputName));
    }

    @Test
    @Order(2)
    void addAvailability_duplicateName_expectException() throws InvalidTimeException, DuplicateUsernameException {
        String inputTime = "12:53";
        String inputName = "shixi";
        meetingManager.addAvailability(inputTime, inputName);
        assertThrows(DuplicateUsernameException.class,
                () -> meetingManager.addAvailability(inputTime, inputName));
    }
}