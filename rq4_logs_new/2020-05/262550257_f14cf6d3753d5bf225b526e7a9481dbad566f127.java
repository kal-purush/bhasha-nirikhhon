package sample;

import org.junit.Test;

import static org.junit.Assert.*;

public class ControllerTest {

    @Test
    public void handleAction() {
    }

    @Test
    public void numberPressed() {
    }

    @Test
    public void pointPressed() {
    }

    @Test
    public void chsPressed() {
    }

    @Test
    public void clearPressed() {
    }

    @Test
    public void plusPressed() {
    }

    @Test
    public void minusPressed() {
    }

    @Test
    public void multiplyPressed() {
    }

    @Test
    public void dividePressed() {
    }

    @Test
    public void enterPressed() {
    }

    @Test
    public void getDisplayValue() {
    }

    @Test
    public void getVersion() {
    }

    @Test
    public void toDouble() {
        Controller controller = new Controller();
        double value = controller.toDouble("1");
        assertEquals(1.0, value, 0);
    }

    @Test
    public void testToString() {
        Controller controller = new Controller();
        String value = controller.toString(1);
        assertEquals("1.0", value);
    }
}