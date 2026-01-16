package covid;
import java.io.*;
import java.net.*;
import java.time.LocalDate;
import java.util.*;

/**
* Fetch and parse WHO Covid-19 data 
* @author Ansor Kasimov
* @version 1.0
*/

public class Application {

		public static void main(String[] args) {
			
			// Store Covid-19 data
			LinkedList<CovidCase> list = new LinkedList<CovidCase>();
			
			// CSV file
			File dataFile = new File("WHO-COVID-19-global-data.csv");
			URL dataURL = null;
			
			try {
				dataURL = new URL("https://covid19.who.int/WHO-COVID-19-global-data.csv");
			} catch (MalformedURLException murle) {
				murle.printStackTrace();
			}
			
			String line;
			
			// Make a new CSV file if there isnt already one in this directory
			if (!dataFile.exists()) {	
				try {
					Scanner sc = new Scanner(dataURL.openStream());
					PrintWriter pw = new PrintWriter(dataFile);
					while (sc.hasNextLine()) {
						line = sc.nextLine();
						pw.println(line);
					}
					sc.close();
					pw.close();
				} catch (IOException ioe) {
					System.out.println("Error creating the file or reading data from URL");
					System.exit(1);
				}
			}
		
			// Parse the data and turn them into instances of CovidCase
			try {
				Scanner sc = new Scanner(dataFile);
				sc.nextLine();
				while (sc.hasNextLine()) {
					line = sc.nextLine();
					String regex = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
					String [] values = line.split(regex); 
					CovidCase cc = new CovidCase(values);
					if ((cc.getDateReported().equals(LocalDate.now())) || (cc.getDateReported().equals(LocalDate.now().minusDays(1))))
						list.add(cc);
				}
				sc.close();
			} catch (FileNotFoundException fnfe) {
				fnfe.printStackTrace();
			}
				
			// 1st question
			Scanner sc = new Scanner(System.in);
			System.out.print("Enter a valid country name or country code (q to quit): ");
			String input = sc.nextLine();
			
			// End if q is entered, else loop questions
			if (input.equals("q")) {
				sc.close();
				System.out.println("\nGoodbye!");
			} else {	
				while (!input.equals("q")) {
					
					// search by country name
					if (input.length() > 2) {
						for (CovidCase cc : list) { 
							if (cc.getCountry().toLowerCase().contains(input.toLowerCase())) {
								System.out.println("\n" + list.get(list.lastIndexOf(cc)));
								break;
							} 
						}
					} 
					
					// search by country code
					else {
						for (CovidCase cc : list) { 
							if (cc.getCountryCode().toLowerCase().equals(input.toLowerCase())) {
									System.out.println("\n" + list.get(list.lastIndexOf(cc)));
									break;
							} 
						} 
					}
					
					System.out.print("\nEnter another valid country name or country code (q to quit): ");
					input = sc.nextLine();

				}
				sc.close();
				System.out.println("\nGoodbye!");
			}
			
		}

	}

