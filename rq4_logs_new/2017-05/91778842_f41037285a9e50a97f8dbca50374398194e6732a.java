import java.util.Random;
import java.util.Scanner;

public class dni {

	public static void main(String[] args) {
		
		char letras[] = { 'T', 'R', 'W', 'A', 'G', 'M', 'Y', 'F', 'P',	'D', 'X', 'B', 'N', 'J', 'Z', 'S',
				'Q', 'V', 'H', 'L', 'C', 'K', 'E' };
Scanner sc= new Scanner(System.in);
System.out.println("introducir el numero de dni");
int numero = sc.nextInt();
sc.nextLine();
char letra=letras[numero&letras.length];

System.out.println("la  letra es: " + letra);


	}

}