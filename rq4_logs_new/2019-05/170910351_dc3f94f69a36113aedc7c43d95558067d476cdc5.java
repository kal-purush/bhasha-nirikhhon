package hse.se.aaizmaylov.petrinetslibrary.utils.math;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IntVectorTest {
    @Test
    void defaultExceptions() {
        assertThrows(IllegalArgumentException.class, () -> new IntVector(0));
        assertThrows(IllegalArgumentException.class, () -> new IntVector(-1));

        IntVector vector = new IntVector(10);

        assertThrows(IndexOutOfBoundsException.class, () -> vector.get(11));
        assertThrows(IndexOutOfBoundsException.class, () -> vector.get(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> vector.set(-1, 2));
        assertThrows(IndexOutOfBoundsException.class, () -> vector.set(11, 2));

        IntVector other = new IntVector(2);

        assertThrows(IllegalArgumentException.class, () -> vector.sum(other));
        assertThrows(IllegalArgumentException.class, () -> vector.subtract(other));
        assertThrows(IllegalArgumentException.class, () -> vector.dot(other));
    }

    @Test
    void sum() {
        IntVector vector1 = new IntVector(new long[] {1, 2, 3});
        IntVector vector2 = new IntVector(new long[] {-1, 2, 23});

        IntVector res = new IntVector(new long[] {0, 4, 26});

        assertEquals(res, vector1.sum(vector2));
    }

    @Test
    void subtract() {
        IntVector vector1 = new IntVector(new long[] {1, 2, 3});
        IntVector vector2 = new IntVector(new long[] {-1, 2, 23});

        IntVector res = new IntVector(new long[] {2, 0, -20});

        assertEquals(res, vector1.subtract(vector2));
    }

    @Test
    void dot() {
        IntVector vector1 = new IntVector(new long[] {1, 2, 3});
        IntVector vector2 = new IntVector(new long[] {-1, 2, 23});

        assertEquals(-1 + 4 + 23 * 3, vector1.dot(vector2));
    }

    @Test
    void inverted() {
        IntVector vector = new IntVector(new long[] {1, 2, 3});

        IntVector res = new IntVector(new long[] {-1, -2, -3});

        assertEquals(res, vector.inverted());
    }

    @Test
    void normalized() {
        IntVector vector = new IntVector(new long[] {10, 1450, -120, 50});

        IntVector result = new IntVector(new long[] {1, 145, -12, 5});

        assertEquals(result, vector.normalized());
    }

    @Test
    void toStringTest() {
        IntVector vector = new IntVector(new long[] {1, -123, 1111});

        assertEquals("(1, -123, 1111)", vector.toString());
    }
}