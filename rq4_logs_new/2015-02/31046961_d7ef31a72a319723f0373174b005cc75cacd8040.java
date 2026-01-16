import java.net.HttpURLConnection;
import java.net.URL;
import java.io.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.util.Scanner;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * An application used to get walking/bicycling times around UCSB campus.
 * 
 * @author Alex Asrian
 *
 *
 */
public class UCSBWalk {

	/**
	 * Main class, gets the starting and ending building and whether the user is biking or walking.
	 * Calls the go method with appropriate parameters.
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	
		System.out.println("Building Codes @ http://registrar.sa.ucsb.edu/locationcodes.htm");
		
		System.out.print("Enter Starting Building Code: ");
	       BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
	       String startBuild = null;
	       try {
	         startBuild = br.readLine();
	       } catch (IOException e) {
	         System.out.println("Error!");
	         System.exit(1);}
		       
		 System.out.print("Enter Destination Building Code: ");
		    BufferedReader br1 = new BufferedReader(new InputStreamReader(System.in));
			String endBuild = null;
			try {
		      endBuild = br1.readLine();
			} catch (IOException e) {
				System.out.println("Error!");
				System.exit(1);}
			
		
										       
			 System.out.print("Mode(bicycling or walking)?: ");
				BufferedReader br4 = new BufferedReader(new InputStreamReader(System.in));
				String mode = null;
				try {
					mode = br4.readLine();
				} 
				catch (IOException e) {
						System.out.println("Error!");
						System.exit(1);
						}
			
												 
		
				
			
				String startLat = null;
				String startLong = null;
				String endLat = null;
				String endLong = null;
				String check = null;
				String check1 = null;
				
				
				
				   File file = new File("LatLong.txt");
			         
			        try {
			         
			            Scanner scanner = new Scanner(file);
			            scanner.useDelimiter(","); 
			            	check = scanner.next();
			            

			            while (!check.equals(startBuild)){
			            	
				           

			            	startLat = scanner.next();
			            	startLong = scanner.next();
			            	
				            	check = scanner.next();

			

			            }
			   
			            
			        } catch (FileNotFoundException e) {
			            e.printStackTrace();
			        }
			        
					   File file1 = new File("LatLong.txt");
				         
				        try {
				         
				            Scanner scanner1 = new Scanner(file1);
				            scanner1.useDelimiter(","); 
		
				            	check1 = scanner1.next();
				            

				  
				            while (!check1.equals(endBuild)){
				            	endLat = scanner1.next();
				            	endLong = scanner1.next();
				            	
					            check1 = scanner1.next();

			

				          }
				   
				            
				        } catch (FileNotFoundException e) {
				            e.printStackTrace();
				        }
			         
	    

		System.out.println("This Route Will Take ...");
		System.out.println(go(startLong, startLat, endLong, endLat, mode));
			        
			        }
			            	
    /**
     *
     * This method takes in proper Lats and Longs and places in a google URL.
     * This returns an XML file which is parsed to find the total time in seconds and minutes.
     *
     */
	
	public static String go(String startLong, String startLat, String endLong, String endLat, String mode){
		Element route, leg, duration;
		String googleURL = "http://maps.googleapis.com/maps/api/directions/xml?origin="+startLat+","+startLong+"&destination="+endLat+","+endLong+"&mode="+mode+"&sensor=false";
		  try{
	            URL urlGoogleDirService = new URL(googleURL);

	            HttpURLConnection urlGoogleDirCon = (HttpURLConnection)urlGoogleDirService.openConnection();
	            urlGoogleDirCon.setAllowUserInteraction( false );
	            urlGoogleDirCon.setDoInput( true );
	            urlGoogleDirCon.setDoOutput( false );
	            urlGoogleDirCon.setUseCaches( true );
	            urlGoogleDirCon.setRequestMethod("GET");
	            urlGoogleDirCon.connect();

	            
	            DocumentBuilderFactory factoryDir = DocumentBuilderFactory.newInstance();
	            DocumentBuilder parserDirInfo = factoryDir.newDocumentBuilder();
	            Document docDir = parserDirInfo.parse(urlGoogleDirCon.getInputStream());
	            urlGoogleDirCon.disconnect();
	            route    = (Element) docDir.getElementsByTagName("route").item(0);
	            leg      = (Element) route.getElementsByTagName("leg").item(0);
	            duration = (Element) leg.getElementsByTagName("duration").item(0);
	            int i = 0;
	            while((Element) leg.getElementsByTagName("duration").item(i) != null){
		            duration = (Element) leg.getElementsByTagName("duration").item(i);
		            i++;
	            }    
	            return duration.getTextContent();
	                                
		
	
		
		
		  }
		  catch(Exception e){
			  System.out.println(e);
			  return null;
		  }
	}
	
	
} 