/*
Escreva um programa que jogue o jogo do par ou ímpar com você.
 */
package estruturadecisao;

import java.util.Scanner;
import java.util.Random;

public class Ex23 {

    public static void main(String[] args) {
        Scanner read = new Scanner(System.in);
        Random sorteio = new Random();
        System.out.println("*** PAR ou ÍMPAR ***");
        System.out.println("1- PAR | 2- ÍMPAR");
        System.out.print("Escolha: ");
        int lado = Integer.parseInt(read.nextLine());
        if (lado < 1 || lado > 2) {
            System.err.println("Escolha inválida!!!");
            System.exit(lado);
        }
        System.out.print("Digite um número: ");
        int numero = Integer.parseInt(read.nextLine());
        int NumeroMaquina = sorteio.nextInt(10);
        System.out.println("Número escolhido pela máquina: " + NumeroMaquina);
        int total = numero + NumeroMaquina;
        System.out.println("Total = " + total);
        System.out.println("-----------------------------------------------");
        if (lado == 1) {
            if (total % 2 == 0) {
                System.out.println("PAR!!!");
                System.out.println("VOCÊ VENCEU!!!!");
            } else {
                System.out.println("ÍMPAR!!!");
                System.out.println("VOCÊ PERDEU!!!!");
            }
        } else if (lado == 2) {
            if (total % 2 == 0) {
                System.out.println("PAR!!!");
                System.out.println("VOCÊ PERDEU!!!!");
            } else {
                System.out.println("ÍMPAR!!!");
                System.out.println("VOCÊ VENCEU!!!!");
            }
        }
    }
}