import java.util.Scanner;

public class Extras1 {
    public static void main(String[] args) {
        Scanner s = new Scanner(System.in);
        System.out.println("Entre com o valor de D:");
        int D = s.nextInt();
        int Q = 4*D-3;
        System.out.println("A quantidade de palitos que há no diagrama " + D + " é: " + Q);
    }
}

/*
Questão 1. Quantos quadrados há no diagrama de número 4?
(A) 10
(B) 12
(C) 13 -> resposta correta
(D) 15
(E) 16
*/