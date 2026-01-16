package challenge;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FunctionTest {

    @org.junit.jupiter.api.Test
    void testOneMinusYSuccess() {
        int r1 = Function.oneMinusY(5);
        System.out.println("r1 = " + r1);

    }

    @Test
    void testOneMinusYError() {
        try {
            int r1 = Function.oneMinusY(16);
            System.out.println("r1 = " + r1);
        } catch (Exception e) {
            assertTrue(e.getClass() == IndexOutOfBoundsException.class);
        }

    }

    @org.junit.jupiter.api.Test
    void testXMinusYSuccess() {
        Function f = new Function(10);
        int r1 = f.xMinusY(5);
        assertTrue(r1 == 5);
    }

    @org.junit.jupiter.api.Test
    void testXMinusYError() {
        try {
            Function f = new Function(10);
            int r1 = f.xMinusY(10);
        } catch (Exception e) {
            assertTrue(e.getClass() == IndexOutOfBoundsException.class);
        }
    }
}