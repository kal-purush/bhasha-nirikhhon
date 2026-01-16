import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Scanner ler = new Scanner(System.in);

        Float numeros[] = new Float[10];
        for (int i = 0; i < 10; i++) {
            System.out.println("Digite o numero");
            numeros[i] = ler.nextFloat();
            System.out.println();
        }

        float soma = 0;
        float menor = numeros[0];
        float maior = numeros[0];
        for (int i = 0; i< 10; i++){
            soma = soma + numeros[i];
            if (numeros[i] < menor)
                menor = numeros[i];

            if (numeros[i] > maior)
                maior = numeros[i];

        }

        float media = soma / numeros.length;

        String resultado = "";
        for(Float aux : numeros) {
            resultado += aux + "\n";

        }

        System.out.println("média: "+ media + " Menor valor: "+ menor + " Maior Valor: "+ maior);
        System.out.println("Vetor: " + resultado);
        }
    }