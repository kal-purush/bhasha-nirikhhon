package pp.babyname.finder;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;

public class NameFindDriver 
{
	
	/*Variables for the program.*/
	public static Scanner scanner;
	private static final Scanner console = new Scanner(System.in);
	private static boolean matchfound, matchfound2, matchfound3;
	private static int sarah, hudson;
	public static PrintWriter writer;
	
	public static void main(String[] args)
	{
		 String content = null;
		  
		 
		 try 
		{
			 
			 
			//content = new Scanner(new FileInputStream("babynames.txt")); 
			
			scanner = new Scanner(new FileInputStream("babynames.txt"));
			
			
			
			/*Tell the user to pick a selection between 1-4*/
			System.out.println("Please enter the name to find in the list: ");
			String name1 = console.nextLine(); //clears the prompt (to skip enter key)
			
			
			/*Tell the user to pick a selection between 1-4*/
			System.out.println("Please enter the name to find in the list: ");
			String name2 = console.nextLine(); //clears the prompt (to skip enter key)
			
			/*Tell the user to pick a selection between 1-4*/
			System.out.println("Please enter the name to find in the list: ");
			String name3 = console.nextLine(); //clears the prompt (to skip enter key)
			
			while(scanner.hasNextLine())
			{
				
				content = scanner.nextLine();
				
				
				if(content.equalsIgnoreCase(name1))
				{
					matchfound = true;
				}
				
				if(content.equalsIgnoreCase(name2))
				{
					matchfound2 = true;
				}
				if(content.equalsIgnoreCase(name3))
				{
					matchfound3 = true;
				}
				
				if(content.equalsIgnoreCase("sarah"))
				{
					sarah++;
				}
				
				if(content.equalsIgnoreCase("hudson"))
				{
					hudson++;
				}
				
			}
			
			System.out.println(name1 + " is found? "+ matchfound);
			System.out.println(name2 + " is found? "+ matchfound2);
			System.out.println(name3 + " is found? "+ matchfound3);
			System.out.println("Sarah is found #: "+ sarah);
			System.out.println("Hudson is found #: "+ hudson);
			
			writer = new PrintWriter(new FileOutputStream("results.txt", true));
			writer.println(name1 + " is found? "+ matchfound);
			writer.println(name2 + " is found? "+ matchfound2);
			writer.println(name3 + " is found? "+ matchfound3);
			writer.println("Sarah is found #: "+ sarah);
			writer.println("Hudson is found #: "+ hudson);
			
			
			
			//content = new String(Files.readAllBytes(Paths.get("babynames.txt")));
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 finally
		 {
			 if(writer !=null)
			 {
				 writer.close();
			 }
			 
			 if(scanner !=null)
			 {
				 scanner.close();
			 }
		 }
		
		 
	}
	

}