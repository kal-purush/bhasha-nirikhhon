import java.util.Calendar;
import java.util.Scanner;
public class countAge {
	public static void main(String[] args){
		Calendar now = Calendar.getInstance();
		int yearnow = now.get(now.YEAR);
		int monthnow = now.get(now.MONTH);
		int datenow = now.get(now.DAY_OF_MONTH);

		Scanner in = new Scanner(System.in);
		System.out.println("Enter date of birth (date, month, year): ");
		int date = in.nextInt();
		int month = in.nextInt();
		int year = in.nextInt();

		double monthFinal = (monthnow - month) / 12.0;
		double dateFinal = (datenow - date) / 365.0;
		double yearFinal = yearnow - year;
		double age = yearFinal + monthFinal + dateFinal;

		System.out.println("The age is " + age);

	}
}