import java.util.Scanner;

class TP402{
    public static void main(String[] args) {
        int i, j, k, a, b, c, jumlah = 0;
        int matriks1[][] = new int[100][100];
        int matriks2[][] = new int[100][100];
        int hasil[][] = new int[100][100];
        Scanner obj = new Scanner(System.in);
        i = obj.nextInt();
        j = obj.nextInt();
        k = obj.nextInt();

        System.out.println("Matriks A: ");
        for (a = 0; a < i; a++) {
            for (b = 0; b < j; b++) {
                matriks1[a][b] = obj.nextInt();
            }
        }
        System.out.println("Matriks B: ");
        for (a = 0; a < j; a++) {
            for (b = 0; b < k; b++) {
                matriks2[a][b] = obj.nextInt();
            }
        }
        for (a = 0; a < i; a++) {
            for (b = 0; b < k; b++) {
                for (c = 0; c < j; c++) {
                    jumlah = jumlah + matriks1[a][c] * matriks2[c][b];
                }
                hasil[a][b] = jumlah;
                jumlah = 0;
            }
        }
        System.out.println("Hasil: ");
        for (a = 0; a < i; a++) {
            for (b = 0; b < k; b++) {
                System.out.print(hasil[a][b] + " ");
            }
            System.out.println();
        }
    }
}
