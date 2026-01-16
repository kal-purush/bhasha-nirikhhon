package Excersises;
import java.util.Scanner;
public class MetricConversion2 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		double value;
		Scanner input = new Scanner(System.in);
		System.out.print("Enter an Integer: ");
		value = input.nextDouble();
		gallonstolitres(value);
		inchestocm(value);

	}
	public static void gallonstolitres(double value) {
		double litres;
		litres = value * 3.7854;
		System.out.println(value + " gallons is equal to " + litres + " liters");
	}
	public static void inchestocm(double value) {
		double cm;
		cm = value * 2.54;
		System.out.print(value + " inches is equal to " + cm + " centimeters");
	}
}