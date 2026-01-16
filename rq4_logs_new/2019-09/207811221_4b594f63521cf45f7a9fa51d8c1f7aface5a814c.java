import java.util.Scanner;

public class Example02 {

	public static void main(String[] args) {
		
		Scanner input=new Scanner(System.in);
		
		
		System.out.println("skriv namn :");
		
		String namn=input.nextLine();
		
		System.out.println("skriv �lder : ");
		int �lder = input.nextInt();
		
		System.out.println("Skriv din l�ngd : ");
		double l�ngd = input.nextDouble();
		
		System.out.println(namn+" �r "+�lder+"�r gammal och har en "+l�ngd+"m l�ng.");
		System.out.println("G�tt");
	}

}