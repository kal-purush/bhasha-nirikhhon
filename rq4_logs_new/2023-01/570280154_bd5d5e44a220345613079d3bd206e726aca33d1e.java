import java.util.Scanner;

public class dia4_array{
  public static void main(String[] args) {
    Scanner sc = new Scanner(System.in);

    int[] numeros = {0,1,2};
    String[] colores = {"rojo","amarillo","verde"};
    int tamaColores = colores.length;


    colores[2] = "azul";
    System.out.println(colores[2]);
    System.out.println(tamaColores);

  }
}