import java.io.*;
import java.util.*;

public class PublicationListingProcess2 {
	
	public static Publication[] PublicationArray;
	public static int numberOfLines;
	public static int arrayPosition = 0;
	
	File outputData = new File("PublicationData_Output.txt");

	public static void main(String[] args) throws IOException {
		
		Scanner in = new Scanner(System.in);
		System.out.println("Opening \"PublicationData_Output.txt\"\n");

		File toBeAppended = new File("PublicationData_Output.txt");
		insertRowsToFile(toBeAppended);

		System.out.println("Displaying updated file:\n");

		printFileItems(toBeAppended);

		// setting PublicationArray to have index number be equal to number of
		// lines in record

		numberOfLines = numberOfItems(toBeAppended);

		PublicationArray = new Publication[numberOfLines];

		writeFileToArray(toBeAppended);
		long pubCode;
		System.out.print("\nEnter a publication code to search for: ");
		pubCode = in.nextLong();
	
		binaryPublicationSearch(PublicationArray, 0, numberOfLines-1, pubCode);
		sequentialPublicationSearch(PublicationArray, 0, numberOfLines-1, pubCode);
		
		
		in.close();
	}
	
	public static void insertRowsToFile(File ootpoot){
		try{
			Scanner in = new Scanner(System.in);
			boolean anotherRecord = true;
			boolean yesOrNo = true;
			String data = null;
			String yesno = null;
			File file = ootpoot;
    		
    		// if file doesnt exist, creates it
    		
    		if(!file.exists()){
    			file.createNewFile();
    		}
    		
			while(anotherRecord){
				
				System.out.println("Enter a new record: ");
				data = in.nextLine();

				// true = append file

				FileWriter fileWritter = new FileWriter(file.getName(), true);
				BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
				bufferWritter.newLine();
				bufferWritter.write(data);
				

				while(yesOrNo){
					
					System.out.print("Do you wish to add another record? (Y/N): ");
					yesno = in.nextLine();
					
					if (yesno.equalsIgnoreCase("Y")) {
						System.out.print("Enter another new record: ");
						data = in.nextLine();
						bufferWritter.newLine();
						bufferWritter.write(data);
					}

					else if (yesno.equalsIgnoreCase("N")) {
						bufferWritter.close();
						anotherRecord = false;
						yesOrNo = false;
					}

					else {
						System.out.print("Invalid input.\n");
					}
				}
				
				
			}

			
	        System.out.println("Closing file writer.");
	        
    	}catch(IOException e){
    		e.printStackTrace();
    	}
		
	}
	
	public static void writeFileToArray(File inFile)
			throws FileNotFoundException, IOException {
		BufferedReader br = new BufferedReader(new FileReader(inFile));

		try {
			String line = "";

			while ((line = br.readLine()) != null) {

				String[] tokens = line.split(" ");
				Publication pub1 = new Publication(Long.parseLong(tokens[0]),
						tokens[1], Integer.parseInt(tokens[2]), tokens[3],
						Double.parseDouble(tokens[4]),
						Integer.parseInt(tokens[5]));
				PublicationArray[arrayPosition] = pub1;
				arrayPosition++;
			}
		} catch (IOException e) {
			// e.printStackTrace();
		}

		br.close();

	}
	
	// Print items from file method
		public static void printFileItems(File aFile) {
			BufferedReader br;
			try {
				br = new BufferedReader(new FileReader(aFile));
				String line;
				System.out.println();
				while ((line = br.readLine()) != null) {
					System.out.println(line);
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public static int numberOfItems(File Filename) throws IOException {
			LineNumberReader reader = new LineNumberReader(new FileReader(Filename));
			int cnt = 0;
			String lineRead = "";
			while ((lineRead = reader.readLine()) != null) {
			}

			cnt = reader.getLineNumber();
			reader.close();
			return cnt;

		}
		
		public static void binaryPublicationSearch(Publication[] anArray, int startIndex, int endIndex, long publicationCode){
			
		long[] codeArray = new long[endIndex];

		for (int i = 0; i < endIndex; i++) {
			codeArray[i] = anArray[i].getPublication_code();
		}

		int position;
		int comparisonCount = 1; // counting the number of comparisons
									// (optional)

		// Find the subscript of the middle position.
		position = (startIndex + endIndex) / 2;

		while ((codeArray[position] != publicationCode)
				&& (startIndex <= endIndex)) {
			comparisonCount++;
			if (codeArray[position] > publicationCode) // If the number is >
														// key, ..
			{ // decrease position by one.
				endIndex = position - 1;
			} else {
				startIndex = position + 1; // Else, increase position by one.
			}
			position = (startIndex + endIndex) / 2;
		}
		if (startIndex <= endIndex) {
			System.out.println("With binary search, the publication code has been found in array index "
					+ position + " after "
					+ comparisonCount + " iteration(s).");
		}

	}

		public static void sequentialPublicationSearch(Publication[] anArray, int startIndex, int endIndex, long publicationCode){
			int count = 1;
			for (int i = 0; i <= endIndex; i++){
				if (anArray[i].getPublication_code() == publicationCode)
				{
					System.out.println("With sequential search, publication code has been found in array index " + i
							+ " after " + count + " iteration(s).");
				}
				else
					count++;
			}
		}


}
