package ru.otus.java.basic.safarov.homework08;

public class Main {

    public static void main(String[] args) {
        String[][] array = {{"1", "1", "1", "1"},
                {"1", "1", "1", "1"},
                {"1", "1", "1", "a"},
                {"1", "1", "1", "1"}
        };
        try {
            System.out.println(sumTwoDimensionalArray(array));
        } catch (AppArraySizeException e) {
            System.out.println(e);
        } catch (AppArrayDataException e) {
            System.out.println(e);
        }
    }

    public static int sumTwoDimensionalArray(String[][] array) throws AppArraySizeException, AppArrayDataException {
        if (array.length != 4 || array[0].length != 4) {
            throw new AppArraySizeException("Размер переданного массива не 4 на 4");
        }
        int result = 0;
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                try{
                    result += Integer.valueOf(array[i][j]);
                } catch (NumberFormatException e) {
//                    throw new AppArrayDataException("Данные в ячейки " + i + " " + j + " не являются числом");
                    System.out.println("Данные в ячейки " + i + " - " + j + " не являются числом");
                }
            }
        }
        return result;
    }
}