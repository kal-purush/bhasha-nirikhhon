import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;
public class chaching {
	
	
	
	static connection connection =new connection();
	
	 public static void  switch_to_json() throws IOException { 

		 System.out.println(connection.getresponse());
	     String Str=connection.getresponse();
	     String jsonn=Str.substring(1, Str.length()-1);
	     System.out.println(jsonn);
	     org.json.JSONObject js=new org.json.JSONObject(jsonn);
	     String name = (String)js.get("name");
	     String capital = (String)js.get("capital");
	     long population = ((Number) js.get("population")).longValue();
	  // Long population1 = (Long)js.get("population");
	     String population1 = Long.toString(population);
	     String all[]= {name,capital,population1};
	      write(all);
		 }
	 
	 public static void  write(String[]array) throws IOException {
		

		        FileWriter writeer = new FileWriter("db.txt", true);

		           	        
		            writeer.write(array[0]);
		            writeer.write("\n");
		            writeer.write(array[1]);
		            writeer.write("\n");
		            writeer.write(array[2]);
		            writeer.write("\n");
                   
		        

		        writeer.close();

		    }
	 
	 
	 
	 
	 
	 
	 public void Read(String Name) throws Exception {

             boolean aExists=false;
	        BufferedReader bufferedReader;

	        bufferedReader = new BufferedReader(new FileReader("db.txt"));

	        String line2;
	        String line1;
	        String line3;

	     

	            while ((line1 = bufferedReader.readLine()) != null & (line2 = bufferedReader.readLine()) != null & (line3 = bufferedReader.readLine()) != null) {

	                if (line1.equals(Name) ) {

	                    aExists = true;

	                    break;
	                }
	            }
	            

	            if (aExists) {
	                System.out.println(line1);
	                System.out.println(line2);
	                System.out.println(line3);
	                
	            } else {
	                connection.call_me(Name);
	                switch_to_json();
	            }
		 
	 
	 
	 }
	 
	 
		 
		 
		 
		 
		 
	 }
	 

	 
	 
	 
	 
	 
	 
