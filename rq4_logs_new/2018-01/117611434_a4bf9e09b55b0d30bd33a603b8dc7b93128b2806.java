import java.io.File;
import java.nio.file.Paths;
import java.util.Scanner;

public class CaesarShifter {
	
	private String cipherText;
	
	private FileRW printer;
	
	public CaesarShifter(FileRW cipherRW, FileRW printer) {
		this.cipherText = cipherRW.readFile();
		this.printer = printer;
		commandsBlock();
	}
	
	public static void main(String[] args) {
		/*String filename;
		if (args.length == 0) {
			System.out.println("Please enter a .txt file name in the assets folder.");
			Scanner scan = new Scanner(System.in);
			String token = scan.next();
			token = token.trim();
			scan.close();
			filename = token;
		}
		else {
			filename = args[0];
		}*/
		FileRW cipherRW = new FileRW(new File(Paths.get("src/assets/source.txt").toString()));
		FileRW printer = new FileRW(new File(Paths.get("src/assets/printFile.txt").toString()));
		
		new CaesarShifter(cipherRW, printer);
	}
	
	public void commandsBlock() {
		Scanner scan = new Scanner(System.in);
		scan.useDelimiter(System.lineSeparator());
		String token = scan.next();
		String command = token.trim();
		while (! command.equals("exit")) {
			if (command.equals("brute force chars")) {
				bruteCharShift();
			} else {
				System.out.println("Unrecognized Command");
			}
			token = scan.next();
			command = token.trim();
		}
		scan.close();
	}
	
	public void bruteCharShift() {
		String[] force = new String[26];
		force[0] = cipherText;
		String workspace;
		StringBuilder shifty = new StringBuilder();
		int workLen;
		for (int i = 1; i < 26; i++) {
			workspace = force[i-1];
			workLen = workspace.length();
			for (int j = 0; j < workLen; j++) {
				char c = workspace.charAt(j);
				if ('a' <= c && c < 'z' || 'A' <= c && c < 'Z') {
					c = (char)(((int) c) + 1);
				} else if (c == 'z') {
					c = 'a';
				} else if (c == 'Z') {
					c = 'A';
				}
				shifty.append(c);
			}
			force[i] = shifty.toString();
			shifty.delete(0, shifty.length());
		}
		for (int k = 0; k < 26; k++) {
			shifty.append(force[k]);
			shifty.append(System.lineSeparator());
		}
		printer.writeToFile(shifty.toString());
	}
}