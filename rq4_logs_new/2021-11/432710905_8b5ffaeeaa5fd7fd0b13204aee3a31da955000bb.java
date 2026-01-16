import java.util.Scanner;

class inversematrix {

    static void getCofactor(int A[][], int temp[][], int p, int q, int n) {
        int i = 0, j = 0;

        for (int row = 0; row < n; row++) {
            for (int col = 0; col < n; col++) {

                if (row != p && col != q) {
                    temp[i][j++] = A[row][col];

                    if (j == n - 1) {
                        j = 0;
                        i++;
                    }
                }
            }
        }
    }

    static int determinant(int A[][], int n) {
        int D = 0;

        if (n == 1)
            return A[0][0];

        int[][] temp = new int[n][n];

        int sign = 1;
        for (int f = 0; f < n; f++) {

            getCofactor(A, temp, 0, f, n);
            D += sign * A[0][f] * determinant(temp, n - 1);

            sign = -sign;
        }

        return D;
    }

    static void adjoint(int A[][], int[][] adj, int r, int c) {
        if (r == 1) {
            adj[0][0] = 1;
            return;
        }
        int sign = 1;
        int[][] temp = new int[r][c];

        for (int i = 0; i < r; i++) {
            for (int j = 0; j < c; j++) {

                getCofactor(A, temp, i, j, r);

                sign = ((i + j) % 2 == 0) ? 1 : -1;

                adj[j][i] = (sign) * (determinant(temp, r - 1));
            }
        }
    }

    static boolean inverse(int A[][], float[][] inverse, int r, int c) {

        int det = determinant(A, r);
        if (det == 0) {
            System.out.print("Determinant is 0, can't find its inverse");
            return false;
        }

        int[][] adj = new int[r][c];
        adjoint(A, adj, r, c);

        for (int i = 0; i < r; i++)
            for (int j = 0; j < c; j++)
                inverse[i][j] = adj[i][j] / (float) det;

        return true;
    }

    static void display(int A[][], int r, int c) {
        for (int i = 0; i < r; i++) {
            for (int j = 0; j < c; j++)
                System.out.print(A[i][j] + " ");
            System.out.println();
        }
    }

    static void display(float A[][], int r, int c) {
        for (int i = 0; i < r; i++) {
            for (int j = 0; j < c; j++)
                System.out.printf("%.6f ", A[i][j]);
            System.out.println();
        }
    }

    public static void main(String[] args) {

        Scanner sc = new Scanner(System.in);
        System.out.println("Enter a no. of Rows");
        int row = sc.nextInt();
        System.out.println("Enter a no. of Columns");
        int col = sc.nextInt();

        if (row != col) {
            System.out.println("A inverse is not possible as no. of rows and column are not equal");
            return;
        }
        int A[][] = new int[row][col];
        System.out.println("Enter Matrix of " + row + " " + "col");
        for (int i = 0; i < row; i++) {
            for (int j = 0; j < col; j++) {
                System.out.print("Enter element no. " + i + " " + j + " ");
                A[i][j] = sc.nextInt();
            }
        }

        int[][] adj = new int[row][col];

        float[][] inv = new float[row][col];

        System.out.print("\nInput matrix is :\n");
        display(A, row, col);

        System.out.print("\nThe Determinant is :\n");
        int det = determinant(A, row);
        System.out.print(det + "\n");

        System.out.print("\nThe Adjoint is :\n");
        adjoint(A, adj, row, col);
        display(adj, row, col);

        System.out.print("\nThe Inverse is :\n");
        if (inverse(A, inv, row, col))
            display(inv, row, col);

    }
}