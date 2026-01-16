import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import dataparser.check.DataCheck;
import java.util.Scanner;


public class DataParser {
	private String filePath;
	private String newFilePath;
	Scanner sc = new Scanner(System.in);
	
	public DataParser() throws IOException {
		filePath = "dados_clima.txt";
		newFilePath = "dados_clima_treated.txt";
	}
	
	public void deleteInvalidLines() throws IOException {
		BufferedReader fileReader = new BufferedReader(new FileReader(filePath));
		BufferedWriter fileWriter = new BufferedWriter(new FileWriter(newFilePath));
		String line = null;
		String line0 = null;
		String vLine[];
		String date = null;
		int turn = 0;
		boolean firstLine = true;
		while((line = fileReader.readLine()) != null) {
			vLine = line.split(";");
			if(turn == 0) {
				line0 = line;
				date = vLine[1];
				++turn;
			} else {
				if(vLine[1].equals(date)) {
					turn = 0;
					if(!firstLine)
						fileWriter.write("\n");
					else
						firstLine = false;
					fileWriter.write(line0);
					fileWriter.write("\n" + line);
				} else {
					date = vLine[1];
					line0 = line;
				}
			}
			
		}
		fileReader.close();
		fileWriter.close();
	}
	
	public void checkNewFile() {
		DataCheck dtCheck = new DataCheck();
		try {
			dtCheck.checkMissingData(newFilePath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void createDefinitiveFiles() throws IOException {
		BufferedReader fileReader = new BufferedReader(new FileReader(newFilePath));
		
		BufferedWriter bwIFile = new BufferedWriter(new FileWriter("dados_clima_input.txt"));
		BufferedWriter bwOFile = new BufferedWriter(new FileWriter("dados_clima_output.txt"));
		
		String line;
		String tempMax = null;
		String tempMin = null;;
		String rainFall = null;;
		String relMeanHumidity = null;
		String windMeanVelocity = null;
		String vLine[];
		
		boolean firstLine =  true;
		int turn = 0;
		int counter = 0;
		while((line = fileReader.readLine()) != null) {
			vLine = line.split(";");
			if(turn == 0) {
				System.out.println("-----00h-----" + counter);
				printVector(vLine);
				
				tempMax = vLine[vLine.length-4];
				relMeanHumidity = vLine[vLine.length-2];
				windMeanVelocity = vLine[vLine.length-1];
				++turn;
			} else {
				System.out.println("12h");
				printVector(vLine);
				
				rainFall = vLine[vLine.length-5];
				tempMin = vLine[vLine.length-3];
				
				if(!firstLine) {
					bwIFile.write("\n");
					bwOFile.write("\n");
				} else
					firstLine = false;
				
				bwIFile.write(tempMax + ";" + tempMin + ";" + relMeanHumidity + ";" + windMeanVelocity);
				bwOFile.write("\n" + rainFall + ";");
				turn = 0;
			}
			++counter;
			sc.nextLine();
		}
		bwIFile.close();
		bwOFile.close();
		fileReader.close();	
	}
	
	public void printVector(String v[]) {
		System.out.println("===============================");
		for(String s : v) {
			System.out.println(s);
		}
		System.out.println("===============================\n");
	}
	
	public static void main(String[] args) throws IOException {
		DataParser parser = new DataParser();
		parser.deleteInvalidLines();
		parser.checkNewFile();
		parser.createDefinitiveFiles();
	}

}