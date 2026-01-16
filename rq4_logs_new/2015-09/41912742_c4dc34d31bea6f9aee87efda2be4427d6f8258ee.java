package filemanagement;

import java.io.IOException;

public class FileData {
	static String filename;
	public FileData(String filename){
		FileData.filename = filename;
	}
	public String[] Read() throws IOException {
		ReadFile file = new ReadFile(filename);
		int number_of_lines = file.readLines();
		String[] Lines = file.OpenFile(number_of_lines);
		return Lines;
	}
	public static void Write(String filepath, String line, boolean append) throws IOException {
		WriteFile data = new WriteFile(filepath, append);
		data.writeToFile(line);
	}
/*	public static void main(String[] args) throws IOException{
		String[] file = Read("/home/david//text.txt");
		for (int i = 0; i < file.length; i++){
			System.out.println(file[i]);
		}
	} */
}