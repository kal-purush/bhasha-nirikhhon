package hse.se.aaizmaylov.petrinetslibrary.utils.math;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IntMatrixTest {
    @Test
    void getters() {
        IntMatrix matrix = new IntMatrix(new long[][] {
                new long[] {1, 2, 3},
                new long[] {3, 5, 6}
        });

        assertEquals(2, matrix.getRows());
        assertEquals(3, matrix.getColumns());

        IntVector secondRow = new IntVector(new long[] {3, 5, 6});
        IntVector secondColumn = new IntVector(new long[] {2, 5});

        assertEquals(secondRow, matrix.getRow(1));
        assertEquals(secondColumn, matrix.getColumn(1));

        assertEquals(3, matrix.get(0, 2));
    }

    @Test
    void multiply() {
        IntMatrix matrix = new IntMatrix(new long[][] {
                new long[] {1, 2, 3},
                new long[] {3, 5, 6}
        });

        IntVector vector = new IntVector(new long[] {3, 2, -5});

        IntVector result = new IntVector(new long[] { 3 + 4 - 15, 9 + 10 - 30 });

        assertEquals(result, matrix.multiply(vector));
    }

    @Test
    void multiplyLeft() {
        IntMatrix matrix = new IntMatrix(new long[][] {
                new long[] {1, 2, 3},
                new long[] {3, 5, 6}
        });

        IntVector vector = new IntVector(new long[] { 2, -5 });

        IntVector result = new IntVector(new long[] { 2 - 15, 4 - 25, 6 - 30 });

        assertEquals(result, matrix.multiplyLeft(vector));
    }

    @Test
    void dotWithRow() {
        IntMatrix matrix = new IntMatrix(new long[][] {
                new long[] {1, 2, 3},
                new long[] {3, 5, 6}
        });

        IntVector vector = new IntVector(new long[] {3, 2, -5});

        assertEquals(9 + 10 -30, matrix.dotWithRow(vector, 1));
    }

    @Test
    void dotWithColumn() {
        IntMatrix matrix = new IntMatrix(new long[][] {
                new long[] {1, 2, 3},
                new long[] {3, 5, 6}
        });

        IntVector vector = new IntVector(new long[] { 2, -5 });

        assertEquals(4 - 25, matrix.dotWithColumn(vector, 1));
    }
}