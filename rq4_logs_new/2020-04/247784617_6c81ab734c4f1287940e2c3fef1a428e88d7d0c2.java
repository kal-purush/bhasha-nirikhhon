import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;
public class All_cached_data {
	
	
	
	static connection connection =new connection();
	
	
	
	 public static void  switch_to_json(String jsonn) throws IOException { 
		
	     
	     org.json.JSONObject js=new org.json.JSONObject(jsonn);
	     String name = (String)js.get("name");
	     String capital = (String)js.get("capital");
	     long population = ((Number) js.get("population")).longValue();
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

	                if (line1.equalsIgnoreCase(Name) ) {

	                    aExists = true;

	                    break;
	                }
	            }
	            

	            if (aExists) {
	            	//String info=line1+line2+line3;
	            	//  org.json.JSONObject js=new org.json.JSONObject(info);
	             //  System.out.println(js);
	                System.out.println("{name:"+line1+",capital:"+line2+",population:"+line3+"}");
	                //System.out.println(line3);
	                
	            } else {
	                connection.call_me(Name);
	               String STR= Response();
	                switch_to_json(STR);
	            }
		 
	 
	 
	 }
	 
	 public String Response() {
		 String Str=connection.getresponse();
   	     String jsonn=Str.substring(1, Str.length()-1);
   	     System.out.println(jsonn);
   	     return jsonn;
	 }
	 
		 
		 
		 
		 
		 
	 }
	 

	 
	 
	 
	 
	 
	 
