package basses;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class CsvReader {
	public HashMap<String,ArrayList<String>> getCsvFileAndIntoHashMap() {
		HashMap<String, ArrayList<String>> basses = new HashMap<String, ArrayList<String>>();
		ArrayList<String> make = new ArrayList<String>();
		ArrayList<String> model = new ArrayList<String>();
		  try(
	             BufferedReader reader = new BufferedReader(new FileReader("./resources/basses.csv"));
	             CSVParser parser = CSVFormat.DEFAULT.withDelimiter(',').withHeader().parse(reader);
	         ) {
	            for(CSVRecord record : parser) {
	            	make.add(record.get("Make"));
	            	model.add(record.get("Model"));         
	            }
	            basses.put("Make", make);
	            basses.put("Model", model);
	            
	        } catch (Exception e) {
	            System.out.println(e);
	        }
		return basses;
	}
	public HashMap<String,ArrayList<String>> getExceptCsvFileInHashMap(){
		HashMap<String, ArrayList<String>> basses = new HashMap<String, ArrayList<String>>();
		ArrayList<String> make = new ArrayList<String>();
		ArrayList<String> model = new ArrayList<String>();
		make.add("Warwick");
		make.add("Warwick");
		make.add("Warwick");
		make.add("Fender");
		make.add("Fender");
		make.add("Yamaha");
		
		model.add("Corvette");
		model.add("Thumb");
		model.add("Streamer");
		model.add("Precision");
		model.add("Jazz");
		model.add("BB500");
		
		basses.put("Make", make);
		basses.put("Model", model);
		
		return basses;
	}
}
