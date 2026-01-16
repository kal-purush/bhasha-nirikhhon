public class App{
	public static void main(String[] args) {
		PrimeSifting newPrimeTest = new PrimeSifting();
		System.out.println(newPrimeTest.getPrimeList(10));
		System.out.println(newPrimeTest.getPrimeList(10).getClass().getName());
	}
}