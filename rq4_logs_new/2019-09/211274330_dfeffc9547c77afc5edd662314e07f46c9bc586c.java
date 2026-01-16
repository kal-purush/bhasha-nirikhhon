package trecidomaci;

import java.util.Scanner;

public class Prvizadatak {

	public static void main(String[] args) {
		Scanner sc = new Scanner(System.in);
		int n, novB = 0, p;
		System.out.println("Unesite broj: ");
		n = sc.nextInt();

		p = n;
		while (n > 0) {
			int c = n % 10;
			novB *= 10;
			novB += c;
			n /= 10;

		}
		if (p == novB) {
			System.out.println("Uneti broj jeste palindrom.");
		} else {
			System.out.println("Uneti broj nije palindrom.");
		}

	}

}