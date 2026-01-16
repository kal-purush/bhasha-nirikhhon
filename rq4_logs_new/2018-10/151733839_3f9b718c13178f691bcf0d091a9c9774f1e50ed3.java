package Main;
import java.util.Scanner;
public class Crossroads 
{

	public static void main(String[] args) 
	{
		int choice;
		String result;
		Scanner input = new Scanner(System.in);
		System.out.println("Where do you want to go? 1 for main village, 2 for forest, 3 for cave, 4 or Dagon's lair");
		choice = input.nextInt();
		if(choice == 1)
		{
			MainVillage.main(null);
		}
		else if(choice == 2)
		{
			Forest.main(null);
		}
		else if(choice == 3)
		{
			Cave.main(null);
		}
		else if(choice == 4)
		{
			DragonsLair.main(null);
		}
		else 
			System.out.println("That is not an option");
	}

}