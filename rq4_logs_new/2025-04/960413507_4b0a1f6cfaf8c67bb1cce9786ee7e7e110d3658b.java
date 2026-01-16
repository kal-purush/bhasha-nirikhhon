import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Uso: java Main num_lin_A num_col_A num_lin_B num_col_B");
            return;
        }

        int numLinA = Integer.parseInt(args[0]);
        int numColA = Integer.parseInt(args[1]);
        int numLinB = Integer.parseInt(args[2]);
        int numColB = Integer.parseInt(args[3]);

        Scanner sc = new Scanner(System.in);

        Matriz A = new Matriz(numLinA, numColA);
        System.out.println("Digite os valores para a matriz A (" + numLinA + "x" + numColA + "):");
        A.leMatriz(sc);
        
        Matriz B = new Matriz(numLinB, numColB);
        System.out.println("Digite os valores para a matriz B (" + numLinB + "x" + numColB + "):");
        B.leMatriz(sc);

        System.out.println("\nMatriz A:");
        A.exibeMatriz();

        System.out.println("\nMatriz B:");
        B.exibeMatriz();

       
        Matriz C = Matriz.matMult(A, B);
        System.out.println("\nMatriz C (resultado da multiplicação de A e B):");
        C.exibeMatriz();

        sc.close();
    }
}