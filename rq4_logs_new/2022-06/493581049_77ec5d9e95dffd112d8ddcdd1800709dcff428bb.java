import java.io.File;
import java.io.FileWriter;
import java.util.Scanner;

public class FileUsingScanner {

	public static void main(String[] args) {
		
		try {
			
			File file=new File("/Users/m.kannan/MINE/zohoClass/Test.txt");
			if(!file.exists()) {
				FileWriter fileWriter=new FileWriter(file);
				Scanner scanner=new Scanner(System.in);
				System.out.println("Enter a String ");
				fileWriter.write(scanner.nextLine());
				System.out.println("File writted sucessfully");
				scanner.close();
				fileWriter.close();	
			}
			else {
				Scanner scanner=new Scanner(file);
				int count=0;
				while(scanner.hasNextLine()) {
					System.out.println(scanner.nextLine());
					count++;
				}
				System.out.println(count);
				scanner.close();	
				
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}