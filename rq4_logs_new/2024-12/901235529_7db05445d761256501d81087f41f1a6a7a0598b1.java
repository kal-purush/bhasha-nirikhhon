public class MatrixAppTests {
    public static void main(String[] args){
        testCalculateAverageAboveDiagonal_case1();
        testCalculateAverageAboveDiagonal_case2();
        testCalculateAverageAboveDiagonal_case3();
        testCalculateAverageAboveDiagonal_case4();
        testCalculateAverageAboveDiagonal_case5();
        testCalculateAverageAboveDiagonal_case6();
    }

    public static double calculateAverageAboveDiagonal(int[][] matrix) {
        int sum = 0;
        int count = 0;
        int n = matrix.length;

        for (int i = 0; i < n; i++) {
            for (int j = i + 1; j < n; j++) {
                sum += matrix[i][j];
                count++;
            }
        }

        return count == 0 ? 0 : (double) sum / count;
    }

    public static void testCalculateAverageAboveDiagonal_case1() {
        int[][] matrix = {
                {1, 2, 3},
                {4, 5, 6},
                {7, 8, 9}
        };
        double result = calculateAverageAboveDiagonal(matrix);
        System.out.println(3.6666666666666665 == result);
    }

    public static void testCalculateAverageAboveDiagonal_case2() {
        int[][] matrix = {
                {1, 0, 0},
                {0, 1, 0},
                {0, 0, 1}
        };
        double result = calculateAverageAboveDiagonal(matrix);
        System.out.println(0.0 == result);
    }

    public static void testCalculateAverageAboveDiagonal_case3() {
        int[][] matrix = {
                {1, 2},
                {3, 4}
        };
        double result = calculateAverageAboveDiagonal(matrix);
        System.out.println(2.0 == result);
    }

    public static void testCalculateAverageAboveDiagonal_case4() {
        int[][] matrix = {
                {1}
        };
        double result = calculateAverageAboveDiagonal(matrix);
        System.out.println(0.0 == result);
    }

    public static void testCalculateAverageAboveDiagonal_case5() {
        int[][] matrix = {
                {1, 2, 3, 4},
                {5, 6, 7, 8},
                {9, 10, 11, 12},
                {13, 14, 15, 16}
        };
        double result = calculateAverageAboveDiagonal(matrix);
        System.out.println(6.0 == result);
    }

    public static void testCalculateAverageAboveDiagonal_case6() {
        int[][] matrix = {
                {0, 0, 0},
                {0, 0, 0},
                {0, 0, 0}
        };
        double result = calculateAverageAboveDiagonal(matrix);
        System.out.println(0.0 == result);
    }
}