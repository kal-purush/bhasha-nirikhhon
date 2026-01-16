import java.util.Scanner; 
public class Grade
	{

		public static void main(String[] args)
			{
				
				//call array list 
				
				
				//change grade 
				Scanner scanner = new Scanner(System.in);
				System.out.println("What is the first name of the student you want to change?");
				String firstName = scanner.nextLine();
				System.out.println("What is the students last name?");
				String lastName = scanner.nextLine();
				System.out.println("What is the students current grade?");
				int currentGrade = scanner.nextInt();
				System.out.println("What do you want to change" + firstName + "'s grade to?");
				char newGrade = scanner.next().charAt(0);
				
				

			}

	}