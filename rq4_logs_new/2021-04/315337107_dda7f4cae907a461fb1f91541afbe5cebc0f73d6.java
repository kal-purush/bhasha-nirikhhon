import java.util.Scanner;

//Escreva um código que diga se o número exibido é PAR ou ÍMPAR

class Main {
  public static void main(String[] args) {
    Scanner teclado = new Scanner(System.in);
    
    System.out.print("Digite um número: ");
    float num = teclado.nextFloat();

    if(num % 2 == 0) {
      System.out.printf("É par");
    } else {
      System.out.println("É impar");
    }
    
  }
}