package aula01;

import java.util.Scanner;

public class Aula01 {

    public static void main(String[] args) {
        Scanner leia = new Scanner(System.in);

        System.out.println("DIGITE A QTD DE GOLS");
        int qg = leia.nextInt();

        System.out.println("DIGITE A QTD DE PASSES CERTOS");
        int qc = leia.nextInt();

        System.out.println("DIGITE A QTD DE PASSES ERRADOS");
        int qe = leia.nextInt();

        int pontos = (qg * 50) + (qc * 10) + (qe * -5);

        if (pontos < 50) {
            System.out.println("PESSIMA PARTIDA");
        } else if (pontos >= 50 && pontos < 100) {
            System.out.println("PARTIDA RUIM");
        } else if (pontos >= 100 && pontos < 150) {
            System.out.println("FEZ O BASICO");
        } else if (pontos >= 150 && pontos < 200) {
            System.out.println("JOGOU BEM");
        }
    }
}
