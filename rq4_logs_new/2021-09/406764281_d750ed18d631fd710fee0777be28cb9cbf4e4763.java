package RoverTest;
import mars.Coordinate;
import mars.Plateau;
import mars.Rover;

import org.junit.Before;
import org.junit.Test;
import org.junit.internal.runners.JUnit4ClassRunner;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.runner.RunWith;

import static java.util.Arrays.asList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static java.util.Arrays.asList;
import static org.hamcrest.core.Is.*;
import static org.junit.Assert.*;

public class Rovertest {

    private Rover rover;

    @Before
    public void initialise() {
        Plateau plateau = new Plateau();
        rover = new Rover(plateau);
        Coordinate coordinate = new Coordinate(0, 0);
    }

    @Test
    @DisplayName("display right.")
    @ParameterizedTest(name = "right")
    @ValueSource(strings = { "R" })
    void rotate_right(String commands, String position) {
    	assertEquals(rover.execute(commands), position);
    }

    @Test
    @DisplayName("display left.")
    @ParameterizedTest(name = "left")
    @ValueSource(strings = { "L" })
    void rotate_left(String commands, String position) {
        assertEquals(rover.execute(commands), position);
    }

    @Test
    @DisplayName("display up.")
    @ParameterizedTest(name = "up")
    @ValueSource(strings = { "RML" })
    public void move_up(String commands, String position) {
    	assertEquals(rover.execute(commands), position);
    }

    @Test
    @DisplayName("display down.")
    @ParameterizedTest(name = "down")
    @ValueSource(strings = { "RMLMLML" })
    public void move_down(String commands, String position) {
    	assertEquals(rover.execute(commands), position);
    }

}