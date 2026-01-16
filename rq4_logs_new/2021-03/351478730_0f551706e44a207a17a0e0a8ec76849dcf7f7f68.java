package JavaCalendar;

import java.util.Scanner;

public class Sum {
	public static void main (String[] args) {
		//입력 : 키보드로 두 수 의 입력을 받는다
		//출력 : 화면에 두 수의 합을 출력한다
		int num1, num2;
		Scanner scanner = new Scanner(System.in);
		String a, b;
		System.out.println("두 수를 입력하세요");
		a = scanner.next();
		b = scanner.next();
		num1 = Integer.parseInt(a);
		num2 = Integer.parseInt(b);
		System.out.println(num1 + ", " + num2);
		System.out.println("두 수의 합은 " + (num1+num2) + " 입니다");
		scanner.close();
		
		
	}
}