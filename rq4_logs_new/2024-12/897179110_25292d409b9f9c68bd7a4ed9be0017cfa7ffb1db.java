import java.util.Scanner;

public class matriks {
    static int[][] matriks1;
    static int[][] matriks2;
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        while (true) {
            System.out.println("=== OPERASI MATRIKS ===");
            System.out.println("1. Penjumlahan Matriks");
            System.out.println("2. Pengurangan Matriks");
            System.out.println("3. Perkalian Matriks");
            System.out.println("4. Inverse Matriks");
            System.out.println("5. Transpose Matriks");
            System.out.println("6. Keluar");
            System.out.print("Pilih Operasi: ");
            int pilih = sc.nextInt();
            if (pilih == 1) {
                penjumlahan();
            } else if (pilih == 2) {
                penguranganMatriks();
            } else if (pilih == 3) {
                perkalianMatriks();
            } else if (pilih == 4) {
                inverseMatriks();
            } else if (pilih == 5) {
                transpose();   
            } else if (pilih == 6) {
                break;
            } else {
                System.out.println("Pilihan Menu Tidak Valid");
            }
            System.out.println();
        }
    }

    static void perkalianMatriks(){
        Scanner sc = new Scanner(System.in);
        int ordo1 , ordo2 , i, j, k;
        while (true) {
            System.out.println("Masukkan ordo matriks 1: ");
            ordo1 = sc.nextInt();
            System.out.println("Masukkan ordo matriks 2: ");
            ordo2 = sc.nextInt();
            matriks1 = new int[ordo1][ordo1];
            matriks2 = new int[ordo2][ordo2];
            if (ordo1 == ordo2) {
                break;
            }else{
                System.out.println("Ordo matriks harus sama\n");
            }
        }
        int hasil [][] = new int[ordo1][ordo1];

        for (i = 0; i < matriks1.length; i++){
            for (j = 0; j < matriks1[i].length; j++){
                System.out.println("Masukkan Elemen Matriks pertama" + "[" + i + "]" + "[" + j + "]" + " : " );
                matriks1[i][j] = sc.nextInt();
            }
        }

        for (i = 0; i < matriks1.length; i++){
            for (j = 0; j < matriks1[i].length; j++){
                System.out.println("Masukkan Elemen Matriks kedua" + "[" + i + "]" + "[" + j + "]" + " : " );
                matriks2[i][j] = sc.nextInt();
            }
        }

        for (i = 0; i < matriks1.length; i++) {
            for (j = 0; j < matriks2[i].length; j++) {
                for (k = 0; k < matriks2.length; k++){
                    hasil[i][j] += matriks1[i][k] * matriks2[k][j];
                }
            }
        }
        System.out.print("Hasil perkalian dari kedua matriks tersebut : " + hasil);
        System.out.println();
    }

    static void transpose(){
        Scanner sc = new Scanner(System.in);
        System.out.print("Masukkan baris matriks: ");
        int baris = sc.nextInt();
        System.out.print("Masukkan kolom matriks: ");
        int kolom = sc.nextInt();
        matriks1 = new int[baris][kolom];

        for (int i = 0; i < matriks1.length; i++){
            for (int j = 0; j < matriks1[i].length; j++){
                System.out.println("Masukkan Elemen Matriks" + "[" + i + "]" + "[" + j + "]" + " : " );
                matriks1[i][j] = sc.nextInt();
            }
        }

        for (int i = 0; i < matriks1.length; i++){
            for (int j = 0; j < matriks1[i].length; j++){
                System.out.print(matriks1[j][i] + " ");
                System.out.println();
            }
        }
        System.out.println();
    }

    static void penguranganMatriks(){
        Scanner sc = new Scanner(System.in);
        int a, b;

        System.out.print("Masukkan jumlah baris matriks: ");
        a = sc.nextInt();
        System.out.print("Masukkan jumlah kolom matriks: ");
        b = sc.nextInt();

        int[][] barisMatriks = new int[a][b];
        int[][] kolomMatriks = new int[a][b];
        int[][] hasilMatriks = new int[a][b];

        System.out.println("Masukkan elemen matriks pertama: ");
        for (int i = 0; i < a; i++) {
            for (int j = 0; j < b; j++) {
                System.out.print("Elemen [" + i + "][" + j + "]: ");
                barisMatriks[i][j] = sc.nextInt();
            }
        }

        System.out.println("Masukkan elemen matriks kedua: ");
        for (int i = 0; i < a; i++) {
            for (int j = 0; j < b; j++) {
                System.out.print("Elemen [" + i + "][" + j + "]: ");
                kolomMatriks[i][j] = sc.nextInt();
            }
        }

        for (int i = 0; i < a; i++) {
            for (int j = 0; j < b; j++) {
                hasilMatriks[i][j] = barisMatriks[i][j] - kolomMatriks[i][j];
            }
        }

        System.out.println("Hasil pengurangan matriks : ");        
        for (int i = 0; i < a; i++) {
            System.out.print("| ");
            for (int j = 0; j < b; j++) {
                System.out.print(barisMatriks[i][j] + " ");
            }
            System.out.print("|");

            // Menampilkan tanda "-" di tengah
            if (i == a / 2) {
                System.out.print(" - ");
            } else {
                System.out.print("   ");
            }

            // Menampilkan Matriks 2
            System.out.print("| ");
            for (int j = 0; j < b; j++) {
                System.out.print(kolomMatriks[i][j] + " ");
            }
            System.out.print("|");

            // Menampilkan tanda "=" di tengah
            if (i == a / 2) {
                System.out.print(" = ");
            } else {
                System.out.print("   ");
            }

            // Menampilkan Hasil Matriks
            System.out.print("| ");
            for (int j = 0; j < b; j++) {
                System.out.print(hasilMatriks[i][j] + " ");
            }
            System.out.println("|"); 
        }
        System.out.println();
    }

    static void inverseMatriks(){
        Scanner sc = new Scanner(System.in);
        int pilih;
        while (true) {
            System.out.println("=== Pilih Ordo Matriks Inverse ===");
            System.out.println("1. 2x2");
            System.out.println("2. 3x3");
            System.out.println("3. Kembali Ke Menu Utama");
            System.out.print("Pilih Ordo Matriks Inverse: ");
            pilih = sc.nextInt();
            if (pilih == 1) {
                inverse2x2();
            }else if (pilih == 2) {
                inverse3x3();
            }else if (pilih == 3) {
                break;
            }else{
                System.out.println("Pilihan tidak valid");
            }
        }
    }
    static void inverse2x2(){
        Scanner sc = new Scanner(System.in);
        double[][] matriks = new double[2][2];
        double[][] invers = new double[2][2];
        double determinan;

        System.out.println("Masukkan elemen matriks 2x2:");
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 2; j++) {
                System.out.print("Elemen [" + i + "][" + j + "]: ");
                matriks[i][j] = sc.nextDouble();
            }
        }

        determinan = (matriks[0][0] * matriks[1][1]) - (matriks[0][1] * matriks[1][0]);

        if (determinan == 0) {
            System.out.println("Matriks tidak memiliki invers karena determinan = 0.");
        } else {
            invers[0][0] = matriks[1][1] / determinan;
            invers[0][1] = -matriks[0][1] / determinan;
            invers[1][0] = -matriks[1][0] / determinan;
            invers[1][1] = matriks[0][0] / determinan;

            System.out.println("Invers matriks:");
            for (int i = 0; i < 2; i++) {
                System.out.print("| ");
                for (int j = 0; j < 2; j++) {
                    System.out.printf("%.2f ", invers[i][j]);
                }
                System.out.println("|");
            }
        }
        System.out.println();
    }
    static void penjumlahan(){
        Scanner scanner = new Scanner(System.in);
        System.out.print("Masukkan jumlah baris matriks: ");
        int baris = scanner.nextInt();
        System.out.print("Masukkan jumlah kolom matriks: ");
        int kolom = scanner.nextInt();

        int[][] matriks1 = new int[baris][kolom];
        int[][] matriks2 = new int[baris][kolom];
        int[][] hasil = new int[baris][kolom];

        System.out.println("Masukkan elemen matriks pertama:");
        for (int i = 0; i < baris; i++) {
            for (int j = 0; j < kolom; j++) {
                System.out.print("Elemen [" + i + "][" + j + "]: ");
                matriks1[i][j] = scanner.nextInt();
            }
        }

        System.out.println("Masukkan elemen matriks kedua:");
        for (int i = 0; i < baris; i++) {
            for (int j = 0; j < kolom; j++) {
                System.out.print("Elemen [" + i + "][" + j + "]: ");
                matriks2[i][j] = scanner.nextInt();
            }
        }

        for (int i = 0; i < baris; i++) {
            for (int j = 0; j < kolom; j++) {
                hasil[i][j] = matriks1[i][j] + matriks2[i][j];
            }
        }

        System.out.println("Hasil penjumlahan matriks:");
        for (int i = 0; i < baris; i++) {
            for (int j = 0; j < kolom; j++) {
                System.out.print("Matriks1[" + i + "][" + j + "] + Matriks2[" + i + "][" + j + "] = " + hasil[i][j] + "\t");
            }
            System.out.println();
        }
    }

    static void inverse3x3(){
        Scanner scanner = new Scanner(System.in);
        double[][] matrix = new double[3][3];

        System.out.println("Masukkan 9 elemen matriks :");
        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix.length; j++) {
                System.out.print("Elemen [" + (i+1) + "][" + (j+1) + "]: ");
                matrix[i][j] = scanner.nextDouble();
            }
        }
        
        double det = determinan(matrix);
        if (det == 0) {
            System.out.println("Matriks tidak memiliki invers (determinan = 0)");
            return;
        }
        double[][] inverse = hitungInvers(matrix);
        
        System.out.println("\nMatriks Invers:");
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                System.out.print(pembulatan(inverse[i][j]) + " ");
            }
            System.out.println();
        }
    }
    public static double pembulatan(double angka) {
        int bulat = (int)(angka * 100);
        return (double)bulat / 100;
    }
    
    public static double determinan(double[][] matrix) {
        double hasil = 0;
        double a = matrix[0][0] * (matrix[1][1] * matrix[2][2] - matrix[1][2] * matrix[2][1]);
        double b = matrix[0][1] * (matrix[1][0] * matrix[2][2] - matrix[1][2] * matrix[2][0]);
        double c = matrix[0][2] * (matrix[1][0] * matrix[2][1] - matrix[1][1] * matrix[2][0]);
        hasil = a-b+c;
        return hasil;
    }
    
    public static double[][] hitungInvers(double[][] matrix) {
        double det = determinan(matrix);
        double[][] kofaktor = new double[3][3];
        
        kofaktor[0][0] = (matrix[1][1] * matrix[2][2] - matrix[1][2] * matrix[2][1]) / det;
        kofaktor[0][1] = -(matrix[0][1] * matrix[2][2] - matrix[0][2] * matrix[2][1]) / det;
        kofaktor[0][2] = (matrix[0][1] * matrix[1][2] - matrix[0][2] * matrix[1][1]) / det;
        
        kofaktor[1][0] = -(matrix[1][0] * matrix[2][2] - matrix[1][2] * matrix[2][0]) / det;
        kofaktor[1][1] = (matrix[0][0] * matrix[2][2] - matrix[0][2] * matrix[2][0]) / det;
        kofaktor[1][2] = -(matrix[0][0] * matrix[1][2] - matrix[0][2] * matrix[1][0]) / det;
        
        kofaktor[2][0] = (matrix[1][0] * matrix[2][1] - matrix[1][1] * matrix[2][0]) / det;
        kofaktor[2][1] = -(matrix[0][0] * matrix[2][1] - matrix[0][1] * matrix[2][0]) / det;
        kofaktor[2][2] = (matrix[0][0] * matrix[1][1] - matrix[0][1] * matrix[1][0]) / det;
        
        return kofaktor;
    }
}