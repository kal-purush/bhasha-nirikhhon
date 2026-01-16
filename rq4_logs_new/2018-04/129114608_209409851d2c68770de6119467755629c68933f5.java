import java.io.*;

public class URLShortener {

	public static void main(String[] args) throws Exception {
		
		urls();{
			System.out.println();
			
		}

	}
	public static String urls() throws Exception {

		String fileName = "UrlsToShortenList.txt";

		FileReader urlToTest = new FileReader(fileName);
		BufferedReader bufferedReader = new BufferedReader(urlToTest);

		String urlLine;
		while((urlLine=bufferedReader.readLine())!=null) {
			
			return urlLine;
			
		}
		bufferedReader.close();
		
		
		
		
		
	}
	
}
