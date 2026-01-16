package microservice;
import static spark.Spark.get;
import static spark.Spark.put;
import com.google.gson.Gson;
import static spark.Spark.port;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;  
import java.sql.DriverManager;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class CatalogMain2   {
	
    public static String catalogIP_Port1 = "localhost:8077"; // ip and port for  catalog microservice
    
	//Connect to sqlite catalog database
	 private  Connection connect() {  
	        // SQLite connection string to the catalog database 
		 	String url = "jdbc:sqlite:catalogdb";  
	        Connection conn = null;  
	        try {  
	            conn = DriverManager.getConnection(url); 
	            System.out.printf("COnnect Success");
	        } catch (SQLException e) {  
	            System.out.println(e.getMessage());  
	        }  
	        return conn;  
	    }  
	 
  
	//Method to search for the books And return the response (for each book return (id,title))
	 public ResultSet search(String topic) {  
		 String sql = "SELECT id,title FROM  products WHERE topic = ?"; // sql query  
	   
	        try{  
	          
	            PreparedStatement pstmt = conn.prepareStatement(sql);  
	            pstmt.setString(1, topic);   
	            ResultSet res =  pstmt.executeQuery();// execute sql
	            	
	           
	            
	            if(!res.isBeforeFirst()) { //not found 
	            	 resultStr = "Not Found";
	            	 return res;
	            }
	            resultStr = "";
	            return res; //return values
	        } catch (SQLException e) {  
	            System.out.println(e.getMessage());  
	        }
	        resultStr = "Not Found";
			return null;  
	    } 
	 
	 
	//Method to return info about the book (title,price,quantity)
	 public ResultSet info(int id) {  
		 String sql = "SELECT title,price,quantity FROM  products WHERE id = ?";  // sql query 
	   
	        try{  
	            
	            PreparedStatement pstmt = conn.prepareStatement(sql);  
	            pstmt.setInt(1, id);  
	            ResultSet res =  pstmt.executeQuery(); //execute query 
	          
	           
	            
	            if(!res.isBeforeFirst()) { //not found
	            	 resultStr = "Not Found";
	            	 return res;
	            }else { 
	            resultStr = "";
	           
	            return res; //return values
	            }
	        } catch (SQLException e) {  
	        
	            System.out.println(e.getMessage());  
	        }
	        resultStr = "Not Found";
			return null;  
	    } 
	 Connection conn = this.connect();  // create just one connection to the database
	
     //Method to convert ResultSet to JSONArray (Also Add resultStr to the Array)
	 public static JSONArray convert(ResultSet resultSet) throws Exception {
		 
		    JSONArray jsonArray = new JSONArray();
		 
		    while (resultSet.next()) {
		 
		        int columns = resultSet.getMetaData().getColumnCount();
		        JSONObject obj = new JSONObject();
		 
		        for (int i = 0; i < columns; i++)
		            obj.put(resultSet.getMetaData().getColumnLabel(i + 1).toLowerCase(), resultSet.getObject(i + 1));
		     
		        
		        jsonArray.put(obj);
		       System.out.println(jsonArray);
		    }
		    if(!resultStr.equals("")) {
	        	jsonArray.put(resultStr);
	        	
		        
	        }
		    return jsonArray;
		
	 }
	
	 
	 //Method to return the available quantity in stock
	 public int quantity(int id) throws NumberFormatException, JSONException, Exception {  
		 String sql = "SELECT quantity FROM  products WHERE id = ?";// sql query
		 
	   
	        try{  
	            
	            PreparedStatement pstmt = conn.prepareStatement(sql);  
	            pstmt.setInt(1, id);  
	            ResultSet res =  pstmt.executeQuery(); //execute query
	            
	          
	            if(res.getInt(1) > 0) {  
	            	System.out.println("quantity = " + res.getInt(1)); //return the available quantity
	 	            return  res.getInt(1);
	            }else  if(!res.next() ) { //not found
	            	
	            	return -1;
	            }else{ //empty
	            	System.out.println("Sold out");
	            	
	            	return 0;
	            }
	        } catch (SQLException e) {  
	        	
	            System.out.println(e.getMessage());  
	        }
	        resultStr = "Not Found";
			return -1;  
	    } 
	 
	 
	 // Method to update the quantity and return the Remaining quantity after the update
	 public String purchase(int id,int quantity) throws NumberFormatException, JSONException, Exception {  
		 String sql = "UPDATE products SET quantity = ? WHERE id = ?"; // sql query  
		 
	   
	        try{  
	           
	            PreparedStatement pstmt = conn.prepareStatement(sql); 
	            pstmt.setInt(1,quantity);
	            pstmt.setInt(2, id);  
	            pstmt.execute(); //no return value
	            
	          
	        } catch (SQLException e) {  
	        	
	            System.out.println(e.getMessage());  
	        }
	       
			return new Gson().toJson("Purchase Successfully  "+ "Remaining = " + quantity);  //return message
	    } 
	
	 public String purchaseFromMain(int id, int quantity)  throws NumberFormatException, JSONException, Exception {
		 
		 URL url2 = new URL("http://"+catalogIP_Port1+"/purchase/" + id);
		 HttpURLConnection con = (HttpURLConnection) url2.openConnection();
			
		 con = (HttpURLConnection) url2.openConnection();
		 con.setDoOutput(true);
		 con.setRequestMethod("PUT");
		 
		
		 // put in the message (quantity -1)
		 OutputStreamWriter out = new OutputStreamWriter(
				    con.getOutputStream());
		        
		 
				out.write("" + quantity);
				out.close();
				
		 //read the response
		 BufferedReader in2 = new BufferedReader(
				  new InputStreamReader(con.getInputStream()));
				String inputLine2;
				StringBuffer content2 = new StringBuffer();
				while ((inputLine2 = in2.readLine()) != null) {
				    content2.append(inputLine2);
				}
				in2.close();
				System.out.println(content2);
				
		if(!("").contentEquals(content2)) {
			return new Gson().toJson("Purchase Successfully  "+ "Remaining = " + quantity);  //return message

		} else {
			return new Gson().toJson("Purchase Error  "+ "Remaining = " + quantity);  //return message

		}
    
		
 } 
	 
	 
	 
	 public static String resultStr = ""; //contain the message if there is to respond to user
	 
	 
	public static void main(String[] args) {
		
		 CatalogMain2 app = new CatalogMain2(); //create an instance of the class
		
		 
		 


		 port(8078); //start the microservice at port 8077
		 
		 
		 //when it receive a get http request for an topic use search method to search for the books 
		 //And return the response 
		 get("/search/:topic", (req, res) -> {
			 res.type("application/json");
			   
			 return convert(app.search(req.params(":topic")));
		 });
		 
		//when it receive a get http request for an id use info method to return info about the book
		 get("/info/:id", (req, res) -> {
			 res.type("application/json");
			   
			 return convert(app.info(Integer.parseInt(req.params(":id"))));
		 });
		 
		//when it receive a get http request for an id for purchase use quantity method to 
		//return the available quantity in stock
		 get("/purchase/:id", (req, res) -> {
			 res.type("application/json");
			   
					 return app.quantity(Integer.parseInt(req.params(":id")));
		
		 });
		 
		//when it receive a put http request for an id for purchase use purchase method to 
		//update the quantity and return the Remaining quantity after the update
		 put("/purchase/:id", (req,res) -> {
			 res.type("application/json");
			 
		
			 
			 return app.purchaseFromMain(Integer.parseInt(req.params(":id")),Integer.parseInt(req.body()));
		 });
		 
		 put("/purchaseConsistancy/:id", (req,res) -> {
			 res.type("application/json");
			 
		
			 
			 return app.purchase(Integer.parseInt(req.params(":id")),Integer.parseInt(req.body()));
		 });
		 


		 }
	
}