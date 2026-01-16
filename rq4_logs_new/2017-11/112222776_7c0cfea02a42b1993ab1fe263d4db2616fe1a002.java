/*
Hacer un programa que emule un juego de adivinar el número. El programa debe generar un número aleatorio entre 1 y 100 (sin mostrarlo al usuario), y el usuario debe introducir un número para intentar adivinar el número. El programa debe responder con “Mi número es menor”, “Mi número es mayor” o “Has adivinado el número”. Solo se deben dar 8 oportunidades. Si se acaban las oportunidades, el programa deber decir “Se te han acabado las oportunidades. El número era “ y se deberá mostrar el número generado.
*/

import java.util.Random;
import java.util.Scanner;

public class Pregunta1
{
  public static void main(String[] args)
    {
      Random aleatorio = new Random(100);
      Scanner lectura = new Scanner(System.in);
      int resultado;
      int concursante;

      System.out.println("Bienvenido sea al juego de azar!\n\n En unos momentos se sacará un número al azar del 1 al 100, usted debe adivinarlo\n");

      System.out.println("\nEscriba el número a continuación");
      concursante = lectura.nextInt();

      resultado = aleatorio.nextInt(100) + 1;

      //PROCESO ALEATORIO
    if (concursante == resultado)
     {
       System.out.println("Felicidades! Has ganado!");
      }
    }
}

/*
C:\Users\danie\Favorites\Programacion\Exame_Final>java Pregunta1
Bienvenido sea al juego de azar!

 En unos momentos se sacará un número al azar del 1 al 100, usted debe adivinarlo


Escriba el número a continuación
16
Felicidades! Has ganado!

*/