/**
 * 1. Faça um programa que peça uma nota, entre zero e dez. Mostre uma mensagem
 * caso o valor seja inválido e continue pedindo até que o usuário informe um valor
 * válido.
 */
package Facul.Lista3;

import java.util.Scanner;

public class Atv1 {
    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        int nota;
        int sair = 0;

        while(sair != -1) {
            System.out.print("Digite uma nota: ");
            nota = in.nextInt();

            if(nota <= 10 && nota >= 0) {
                System.out.println("A nota foi: "+nota);
                break;
            } else {
                System.out.println("Nota inválida...");
            }
            System.out.print("Digite -1 para sair: ");
            sair = in.nextInt();
        }
    }
}