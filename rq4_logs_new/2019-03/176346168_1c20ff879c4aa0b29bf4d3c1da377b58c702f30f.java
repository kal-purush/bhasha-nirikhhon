package util;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class CSVReader {
	private List<String> file;
	private Scanner scanner;
	
	public CSVReader(String file) {
		this.file = new ArrayList<>();
		try {
			this.scanner = new Scanner(new File(file));
			this.scanner.useDelimiter(";");
			while(this.scanner.hasNextLine())
				this.file.add(this.scanner.nextLine());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.println("lol pas toruvé");
		}
	}
	
	public String toString() {
		return this.file.toString();
	}
	
	public String getLine(int i) {
		return this.file.get(i);
	}
}