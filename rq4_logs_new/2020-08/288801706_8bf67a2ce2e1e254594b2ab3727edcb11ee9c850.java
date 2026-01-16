package lesson_2_2;

public class Main {

    static final int SIZE = 4;

    public static void main(String[] args) {
        String[][] arr = {
                {"1", "2", "3", "4"},
                {"1", "2", "3", "4"},
                {"1", "2", "3", "4"},
                {"1", "2", "3", "4"},
        };

        try {
            System.out.println(sum(arr));
        } catch (MyArraySizeException exception) {
            exception.printStackTrace();
        } catch (MyArrayDataException exception) {
            exception.printStackTrace();
            System.out.println("Position of element: " + exception.getRow() + " " + exception.getColumn());
            System.out.println("Element: " + arr[exception.getRow()][exception.getColumn()]);
        }

    }

    static int sum(String[][] arr) throws MyArrayDataException, MyArraySizeException {
        if (arr.length != SIZE) {
            throw new MyArraySizeException();
        }

        int sum = 0;
        for (int i = 0; i < arr.length; i++) {
            if (arr[i].length != SIZE) {
                throw new MyArraySizeException();
            }
            for (int j = 0; j < arr[i].length; j++) {
                try {
                    sum += Integer.parseInt(arr[i][j]);
                } catch (NumberFormatException exception) {
                    throw new MyArrayDataException("format " + i + " " + j, i, j);
                }
            }
        }
        return sum;
    }

}