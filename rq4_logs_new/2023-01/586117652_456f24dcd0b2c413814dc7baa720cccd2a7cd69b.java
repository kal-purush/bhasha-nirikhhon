/* 
 * Author:    Richfield Quist
 * Course:    COP3503 
 * Project #: 2 
 * Title  :   Data Processing
 * Due Date:  07/10/2022  
 * 
 * Reads in a file provided by user and writes it to another file
 */ 
 
import java.io.*;
import java.text.*;
import java.util.*;


public class CopyPaste {
	
	public static ArrayList<String> dates = new ArrayList<String>();
	public static ArrayList<String> times = new ArrayList<String>();
	public static ArrayList<Double> sensor2278 = new ArrayList<Double>();
	public static ArrayList<Double> sensor3276 = new ArrayList<Double>();
	public static ArrayList<Double> sensor4689 = new ArrayList<Double>();
	public static ArrayList<Double> sensor5032 = new ArrayList<Double>();
	public static ArrayList<Double> section1Diff = new ArrayList<Double>();
	public static ArrayList<Double> section2Diff = new ArrayList<Double>();
	public static ArrayList<Double> totalAvg = new ArrayList<Double>();
	public static ArrayList<String> headerList = new ArrayList<String>();
	
	
	public static void main(String[] args) {

		
		System.out.println("Project 2 Data Preprocessing\n");
		
		//Scanner variable
		Scanner scrn = new Scanner(System.in);
		boolean quit = false;
		
		while(!quit)
		{
			
				
				//Prompt user to input filename and location
				System.out.println("Enter file name & location.");
				String input = scrn.next();
				System.out.println("Reading in Data from the file " + input);

			try {
				//Read in data from file
				FileReader inReader = new FileReader(new File(input));
				Scanner fileInput = new Scanner(inReader);
				fileInput.useDelimiter(",|\\n");
				
				//Read first line which is string of columns
				String[] header = fileInput.nextLine().split(",");
				addToList(fileInput);
				
				//convert date format
				System.out.println("Converting Dates from MM/DD/YYYY to YYYY/MM/DD");
				convertDate();
				
				//Calculating difference
				System.out.println("Calculating Speed Difference");
				calcDiff1();
				calcDiff2();
				
				//Calculating average
				System.out.println("Calculating Speed Average");
				calcAvg();
				
				//Writing data to file
				System.out.println("Writing data to file "+ input +"_Difference.csv");
				FileWriter outWriter = new FileWriter(input+"_Difference.csv");
				PrintWriter output = new PrintWriter(outWriter);
				writeFile(output);
				
				
				quit = true;
				System.out.println("Done! Exiting program");
				
				
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				System.out.println("*File does not exist or path was entered incorrectly.*");
				System.out.println("Please try again");
				
			} catch(NumberFormatException e) {
				System.out.println("*Bad Number Data in CSV File.*");
				System.out.println("Check CSV file data and try again.");
				formatList();
			} catch(ParseException e) {
				System.out.println("*Bad Date Data in CSV File.*");
				System.out.println("Check CSV file data and try again.");
				formatList();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
		
		
	}
	
	
	/** 
     * Writes array list to the new file
     * @param output The printWriter that writes the new array lists on the new file
     * @return null
     */ 
	public static void writeFile(PrintWriter output) {
		// TODO Auto-generated method stub
		
		output.println("Date,Time,Sensor_2278,Sensor_3276,Sensor_4689,Sensor_5032,Section1_Diff,Section2_Diff,Total_Avg\n");
		for(int i=0;i<dates.size();i++) {
			output.println(dates.get(i)+","+times.get(i)+","+sensor2278.get(i)+","+sensor3276.get(i)+","
			+sensor4689.get(i)+","+sensor5032.get(i)+","+section1Diff.get(i)+","+section2Diff.get(i)+","+totalAvg.get(i)+"\n");
		}
		
		
	}


	/** 
     * Calculates average between all sensors
     * @param none
     * @return null
     */ 
	public static void calcAvg() {
		// TODO Auto-generated method stub
		double avg, length = dates.size();
		
		for(int i = 0; i < length; i++) {
			avg = (sensor2278.get(i) + sensor3276.get(i) + sensor4689.get(i) + sensor5032.get(i))/4;
			totalAvg.add(avg);
		}
	}
	
	

	/** 
     * Calculates Difference between sensor5032 and sensor4689
     * @param none
     * @return null
     */ 
	public static void calcDiff2() {
		// TODO Auto-generated method stub
		double difference;
		
		int length = sensor2278.size();		
		for(int i = 0; i < length; i++) {
			difference = sensor4689.get(i) - sensor5032.get(i);
			section2Diff.add(difference);
			
		}
	}

	
	/** 
     * Calculates Difference between sensor3276 and sensor2278
     * @param none
     * @return null
     */ 
	public static void calcDiff1() {
		// TODO Auto-generated method stub
		double difference;
		
		int length = sensor2278.size();		
		for(int i = 0; i < length; i++) {
			difference = sensor2278.get(i) - sensor3276.get(i);
			section1Diff.add(difference);
			
		}
	}

	
	/** 
     * Formats array in case there is any exception
     * @param none
     * @return null
     */ 
	public static void formatList() {
		// TODO Auto-generated method stub
		dates.clear();
		times.clear();
		sensor2278.clear();
		sensor3276.clear();
		sensor4689.clear();
		sensor5032.clear();
		section1Diff.clear();
		section2Diff.clear();
		totalAvg.clear();
	}

	/** 
     * Adds Input read from file to the array list 
     * @param fileReader The fileReader element that takes in the next input
     * @return null
     */ 
	public static void addToList(Scanner fileInput)throws NumberFormatException {
		
		
		
		
		
		while(fileInput.hasNext()) 
		{
			dates.add(fileInput.next());
			
			times.add(fileInput.next());
			
			sensor2278.add(Double.parseDouble(fileInput.next()));
			
			sensor3276.add(Double.parseDouble(fileInput.next()));
			
			sensor4689.add(Double.parseDouble(fileInput.next()));
			
			sensor5032.add(Double.parseDouble(fileInput.next()));
			
		}
		
		
	}
	
	/** 
     * Converts date format from  MM/DD/YYYY to YYYY/MM/DD
     * @param fileInput The scanner that takes in the next input
     * @return null
     */ 
	public static void convertDate() throws ParseException{
		String dateString = "";
		
		SimpleDateFormat currentFormat = new SimpleDateFormat("MM/dd/yyyy");
		SimpleDateFormat newFormat = new SimpleDateFormat("yyyy/MM/dd");
		
		for(int i= 0; i < dates.size();i++ ) {
			dateString = dates.get(i); 
			
			Date dateObject = currentFormat.parse(dateString);
			dateString = newFormat.format(dateObject);
			dates.set(i, dateString);
			
		}
		
		
	}
	
	
}
