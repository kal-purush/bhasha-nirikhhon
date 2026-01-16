import java.util.HashSet;
import java.util.Scanner;

public class CognateWords {

	public static void main(String[] args) {
		Scanner scanner = new Scanner(System.in);
		String words[] = scanner.nextLine().split("[^a-zA-Z]+");
		int inputLength = words.length;	
		HashSet<String> cognateWords = new HashSet<String>();
		
		for (int a = 0; a < inputLength; a++) {
			for (int b = 0; b < inputLength; b++) {
				for (int c = 0; c< inputLength; c++) {
					if (a != b) {
						boolean check = (words[a] + words[b]).equals(words[c]);
						if (check) {
							cognateWords.add(words[a] + "|" + words[b] + "=" + words[c]);
						}
					}
				}
			}
		}
		
		if (cognateWords.isEmpty()) {
			System.out.println("No");
		} else {
			for (String cognate : cognateWords) {
				System.out.println(cognate);
			}
		}
	}
}