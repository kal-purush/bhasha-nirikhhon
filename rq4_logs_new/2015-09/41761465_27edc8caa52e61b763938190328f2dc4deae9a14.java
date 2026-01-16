package Programming;

import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Scanner;

public class GoogolString {
	
	public static StringBuilder switchCharacters(StringBuilder ReversedStringBuilder) {
				
		for(int i=0; i<ReversedStringBuilder.length(); i++) {
			
			//System.out.println(ReversedStringBuilder.charAt(i));
			
			if (ReversedStringBuilder.charAt(i) == '0') {
				
				//System.out.println("Hi");
				ReversedStringBuilder.setCharAt(i, '1');
			}
			else {
				ReversedStringBuilder.setCharAt(i, '0');
			}
			
			//System.out.println(ReversedStringBuilder.charAt(i));
			//System.out.println("Next");
			
		}
		
		return ReversedStringBuilder;
	}
	
	public static StringBuilder googolString(StringBuilder Base, int number) {
		
		// save the initial base value
		StringBuilder InitialBase = new StringBuilder(Base.toString());
		// reverse the initial base
		StringBuilder ReversedBase = Base.reverse();
		// switch 0 to 1 and 1 to 0		
		StringBuilder SwitchedReversedBase = switchCharacters(ReversedBase);
		
		// create the new base
		StringBuilder NewBase = new StringBuilder(InitialBase.toString() + "0" + SwitchedReversedBase.toString());
		
		// do the switch
		if (number == 0) {
			return NewBase;
		}
		
		else {
			return googolString(NewBase, number-1);
		}
		
	}
	
	public static char getCharacter(int CharacterK) {		
		
		StringBuilder Base = new StringBuilder("");
		
		StringBuilder NewBase = googolString(Base, 4);
		
		return NewBase.charAt(CharacterK);		
		
	}

	public static void main(String[] args){
		
		Scanner UserInput = new Scanner(System.in);
		
		int TestCases = UserInput.nextInt();
		
		for(int cases=1; cases <= TestCases; cases++) {
			
			Scanner UserInputCase = new Scanner(System.in);
			int CharacterK = UserInputCase.nextInt();
			
			System.out.println(getCharacter(CharacterK));
		}
	}
}