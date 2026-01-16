package ru.otus.java.basic.safarov.homework08;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MainTest {

    @Test
    void sumTwoDimensionalArrayExpectedAppArraySizeException() {
        String[][] array = {{"1", "1", "1", "1"}, {"1", "1", "1", "1"}};
        AppArraySizeException thrown = assertThrows(AppArraySizeException.class, () -> {
            Main.sumTwoDimensionalArray(array);
        }, "AppArraySizeException was expected");

        assertEquals("Размер переданного массива не 4 на 4", thrown.getMessage());
    }

    @Test
    void sumTwoDimensionalArray() throws AppArrayDataException, AppArraySizeException {
        String[][] array = {{"1", "1", "1", "1"},
                {"1", "1", "1", "1"},
                {"1", "1", "1", "1"},
                {"1", "1", "1", "1"},
        };
        assertEquals(16, Main.sumTwoDimensionalArray(array));
    }

    @Test
    void sumTwoDimensionalArrayExpectedAppArrayDataException() {
        String[][] array = {{"1", "1", "1", "1"},
                {"1", "1", "1", "1"},
                {"1", "1", "a", "1"},
                {"1", "1", "1", "1"},
        };        AppArrayDataException thrown = assertThrows(AppArrayDataException.class, () -> {
            Main.sumTwoDimensionalArray(array);
        }, "AppArrayDataException was expected");

        assertEquals("Данные в ячейки 2 2 не являются числом", thrown.getMessage());
    }
}