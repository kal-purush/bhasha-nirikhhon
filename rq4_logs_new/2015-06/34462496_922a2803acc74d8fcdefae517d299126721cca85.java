import java.util.Scanner;
//imports the scanner so we can use user driven stings
public class Palindrome 
{

	public static void main(String[] args) 
	{
		Scanner scanner = new Scanner(System.in);
		System.out.println("Please input a word or phrase so I can test to see if it is a Palindrome.");
		String InitialString = scanner.nextLine();
		System.out.println("I read that as " + InitialString);
		//this part sets the user response as our initial string.
		scanner.close();
		
		boolean InitialStringPalindrome = ispalindrome(InitialString);
		if(InitialStringPalindrome)
		{
			System.out.println(InitialString + " is a Palindrome.");
		}
		else
		{
			System.out.println(InitialString + " is not a Palindrome.");
		}
		//This part lets the user know if the InitialString is a palindrome.
		System.out.println("Thank You for using this program!");
		//This part just thanks the user for using the program.
	}
	
	public static boolean ispalindrome(String InitialString)
	{
		boolean isPalindrome = true;
		
		if(InitialString != null)
		{
			String InitialStringNoSpace = InitialString.replace(" ", "").toLowerCase();
			//this makes all the characters lower case and gets rid of any spaces.
			char[] InitialStringChars = InitialStringNoSpace.toCharArray();
			//this creates an array for the characters in the string.
			
			for(int i = 0, k = InitialStringChars.length - 1; i < InitialStringChars.length/2; i++, k--)
				//this part initializes a loop that creates a new array based on the InitialString.
			{
				if(InitialStringChars[i] != InitialStringChars[k])
					//this is a loop that checks to see if the characters in the first and second array match.
				{
					isPalindrome = false;
					break;
				}
			}
		}
		
		else
		{
			System.out.println("You gave me a null String.");
			isPalindrome = false;
		}
		
	return isPalindrome;
	}

}