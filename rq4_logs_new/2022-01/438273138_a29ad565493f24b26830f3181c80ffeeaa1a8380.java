package zavrsnirad.startentitet;

import java.util.Scanner;

public class StartProgram {
	private Scanner scanner;

	public StartProgram() {
		scanner = new Scanner(System.in);
		program();
		scanner.close();
	}

	private void program() {
		naslov();
		izbornik();
	}

	private void izbornik() {
		System.out.println("\n1. Djelatnik");
		System.out.println("2. Korisnik");
		System.out.println("3. Usluga");
		System.out.println("4. Posjeta");
		System.out.println("5. Posjeta_usluga");
		System.out.println("6. Izlaz iz programa");
		switch (Unos.unesiInt(scanner, "Unesite akciju")) {
		case 1:
			break;
		case 2:
			break;
		case 3:
			break;
		case 4:
			break;
		case 5:
			break;
		case 6:
			break;
		default:
		}
	}

	private void naslov() {
		System.out.println("******************************");
		System.out.println("**** Program Recepcija V1 ****");
		System.out.println("******************************");
	}

}