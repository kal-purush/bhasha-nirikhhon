package org.fatec;

import java.util.Scanner;

public class Controle {
	private Scanner scanner;
	
	public Controle() {
		this.scanner = new Scanner(System.in);
	}
	
	public int option() {
		int op = scanner.nextInt();
		return op;
	}
	
	public String texto() {
		String t = scanner.nextLine();
		return t;
	}
	
	@Override
	protected void finalize() throws Throwable {
		scanner.close();
	}
}