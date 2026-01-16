package train;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import BasicOps.FileOps;

public class tmp {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String s = "./train/questions";
		try {
			BufferedReader b = new BufferedReader(new FileReader(new File(s)));
			StringBuilder sb = new StringBuilder();
			String line;
			int count = 0;
			while((line = b.readLine()) != null){
				++count;
				sb.append(count);
				sb.append(": ");
				sb.append(line);
				sb.append("\n");
			}
			
			FileOps.SaveFile("./train/print.txt", sb.toString());
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}