class Homework1 {
	byte a1 = 120;
	short b2 = 300;
	boolean c3 = true;
	char d4 = 'A';
	int e5 = 8;
	float f6 = 120.0f;
	double g7 = 15.72732737;
	long h8 = -329482739;
	
	public static void main(String[] args) {
		System.out.print("Zadanie number 3: ");
		calc(10, 4, 2, 1);
		System.out.println("Zadanie number 4: " + checkRange(17, 4));
		System.out.print("Zadanie number 5: ");
		cheсkOnPositive(0);
		System.out.println("Zadanie number 6: " + cheсkOnNegative(-3));
		sayHello("Marina");
		System.out.print("Zadanie number 8: ");
		defineLeapYear(1700);

	}
	
	private static void calc(int a, int b, int c, int d) {
		System.out.println(a * (b + (c/d)));
	}
	
	private static boolean checkRange(int a, int b) {
		if ((a+b)>=10 && (a+b)<=20) return true; else return false;
	}
	
	private static void cheсkOnPositive(int a) {
		if (a >= 0) System.out.println("positiv"); else System.out.println("not positiv");
	}
	
	private static boolean cheсkOnNegative(int a) {
		if (!(a >= 0)) return true; else return false;
	}
	
	private static void sayHello(String a) {
		System.out.println("Zadanie number 7: Hello, " + a + "!");
	}
	
	private static void defineLeapYear(int year){
		
		if ((year % 4 == 0) && (!(year % 100 == 0))) System.out.println("leap  year"); 
		
		else if ((year % 100 == 0) && (year % 400 == 0)) System.out.println("leap  year"); else System.out.println("no leap year");	

	}
}