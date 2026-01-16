import java.util.Scanner;

public class Exercicio2b{
   public static void main(String args []){
        Scanner teclado = new Scanner(System.in);

       double base, altura, resultado; 
       
       System.out.println("Calculando um retangulo");
       System.out.println("Entre com o valor da Base: ");
       base = teclado.nextDouble();
        System.out.println("Entre com o valor da Altura: ");
       altura = teclado.nextDouble();
       resultado = base * altura;
        System.out.println("A area do seu retangulo e: " + resultado);
    }
}