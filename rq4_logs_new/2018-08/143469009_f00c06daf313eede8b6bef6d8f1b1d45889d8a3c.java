package aula10;

import java.util.Scanner;

public class Exercicio1 {

    public static void main(String[] args) {
        Scanner entrada = new Scanner(System.in);

        System.out.println("Digite a quantidade de presentes:");
        int tamanho = entrada.nextInt();

        String[] presentes = new String[tamanho];

        for (int i = 0; i < presentes.length; i++) {
            System.out.println("Digite o nome do presente:");
            presentes[i] = entrada.next();
        }

        // comentário de linha
        /*comentário de bloco*/
        /** comentário de documentação*/
        System.out.println(presentes.length);
        for (String presente : presentes) {
            System.out.println(presente);
        }
    }
}