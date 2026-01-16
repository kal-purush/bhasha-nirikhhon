import java.util.Scanner;

public class Sort {


	
	public static void run()
	{
		System.out.println("Sort Students:");
		System.out.println("1) sort by last name");
		System.out.println("2) sort by GPA");
		System.out.println("3) sort by period");
		Scanner userInput = new Scanner(System.in);
		int option = userInput.nextInt();
		
		if(option == 1)
		{
			lastName();
		}
		else if(option == 2)
		{
			GPA();
		}
		else if(option == 3)
		{
			period();
		}
	}
	
	public static void lastName()
	{
		System.out.println("last name");
	}
	
	public static void GPA()
	{
		System.out.println("GPA");
	}
	
	public static void period()
	{
		System.out.println("period");
	}

}