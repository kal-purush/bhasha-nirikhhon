package hw2;

public class Matrix {
    public static void main(String[] args) {
        String taskString = "10 3 1 2\n2 3 2 2\n5 6 7 1\n300 3 1 0";

        try {
            String[][] numbers = stringTo4x4Matrix(taskString);
            int[][] result = stringMatrixToInt(numbers);

            for (int i = 0; i < result.length; i++) {
                for (int j = 0; j < result[i].length; j++) {
                    System.out.print(result[i][j] + " ");
                }

                System.out.println();
            }
        } catch (MatrixException | IntMatrixException e) {
            e.printStackTrace();
        }
    }

    private static String[][] stringTo4x4Matrix(String str) throws MatrixException {
        String[][] result = new String[4][4];
        String[] rows = str.split("\n+");
        String[] numbers;

        if (rows.length != 4)
            throw new MatrixException(rows.length);

        for (int i = 0; i < rows.length; i++) {
            numbers = rows[i].split(" +");
            if (numbers.length != 4)
                throw new MatrixException(numbers.length, i);

            System.arraycopy(numbers, 0, result[i], 0, numbers.length);
        }

        return result;
    }

    private static int[][] stringMatrixToInt(String[][] matrix) throws IntMatrixException {
        int[][] result = new int[4][4];

        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                try {
                    result[i][j] = Integer.parseInt(matrix[i][j]);
                } catch (NumberFormatException e) {
                    throw new IntMatrixException(i, j);
                }
            }
        }

        return result;
    }
}