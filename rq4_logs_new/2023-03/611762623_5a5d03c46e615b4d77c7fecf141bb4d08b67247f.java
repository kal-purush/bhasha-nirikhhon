import java.util.Random;
import java.util.Scanner;

public class Main {

	public static void main(String[] args) {
		String[] Words = {"OTHER", "ABOUT", "WHICH", "THEIR", "EVERY", "FAITH", "LUNCH", "MAYBE", "DRESS", "BIKES", "FRUIT", "CHOKE", "SPIKY", "MILKY", "GRADE"};
		Random rand = new Random();
		int idx = rand.nextInt(15);
		System.out.println("================[ WORDLE ]================");
		System.out.println("                  RULES!");
		System.out.println("Guess The 5 Letter Words Within 6 Tries");
		System.out.println("G = Correct Letter, Correct place");
		System.out.println("Y = Correct Letter, Wrong place");
		System.out.println("X = Incorrect");
		System.out.println("All the letters needs to be in capital!");
		System.out.println("===============[ HAVE FUN ]===============");
		Scanner sc = new Scanner(System.in);
		int counter = 1;
		
		while(counter <= 6) {
			System.out.printf("Try %d :\n", counter);
			String Guess = sc.next();
//			String Guess = String.valueOf(sc.next().charAt(0));
			if(Guess.length()==5) {				
				counter++;
				int corr = 0;
				for(int j = 0; j < 5; j++) {				
					boolean x = Words[idx].contains(String.valueOf(Guess.charAt(j)));
					boolean y = String.valueOf(Words[idx].charAt(j)).contains(String.valueOf(Guess.charAt(j)));
					if(x == true && y == false) {
						System.out.print("Y");
					}else if(x == true && y == true) {
						System.out.print("G");
						corr++;
					}else {
						System.out.print("X");
					}
				}
				System.out.println("\n==================================");
				if(corr == 5) {
					System.out.println("==[ YOU WON, Congratulations!! ]==");
					break;
				}
			}else {
				System.out.println("MUST BE LENGTH OF 5!");
			}
			
		}
		if(counter == 7) {
			System.out.println("==[ You Lose, SKILL ISSUE ]== ");
		}
		
	}
}