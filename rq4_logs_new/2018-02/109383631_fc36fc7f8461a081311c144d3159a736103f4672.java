package rbccps.smartcity.IDEAM.registerapi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;	
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.json.simple.JSONObject;

import rbccps.smartcity.IDEAM.registerapi.kong.Register;
import rbccps.smartcity.IDEAM.registerapi.parser.createEntityJSONParser;

@Path("/newregister")
public class RequestController {

	private static final createEntityJSONParser DeleteEntityJSONParser = null;

	Register register = new Register();
	
	static JSONObject responseObj;
	static String response;
	static String authorization;
	static String X_Consumer_Custom_ID;
	static String X_Consumer_Username;
	static String apikey;
	static String X_Consumer_Groups;
	static String body;
	
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	public String getAPIKey(@Context HttpServletRequest request) {
		
		System.out.println("------------");
		System.out.println(request.getRequestURI());
		System.out.println("------------");
		
		try {
			getHeaderInfo(request);
			body = getBody(request);
			// System.out.println(body);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		String returnData = createEntityJSONParser.JSONParser();
		System.out.println(returnData);	
		
		return returnData;
	}
	
	@DELETE
	@Produces(MediaType.APPLICATION_JSON)
	public String deleteAPIKey(@Context HttpServletRequest request) {
		
		try {
			getHeaderInfo(request);
			body = getBody(request);
			System.out.println(body);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		String returnData = DeleteEntityJSONParser.JSONParser();
		System.out.println(returnData);	
		
		return returnData;
	}
	
	private void getHeaderInfo(HttpServletRequest request) {
		// TODO Auto-generated method stub
		System.out.println("In Request Header");
		
		System.out.println("------------HEADERS----------------");
		
		authorization = request.getHeader("authorization");
		System.out.println(authorization);
		
		X_Consumer_Custom_ID = request.getHeader("X-Consumer-Custom-ID");
		System.out.println(X_Consumer_Custom_ID);
		
		X_Consumer_Username = request.getHeader("X-Consumer-Username");
		System.out.println(X_Consumer_Username);
		
		apikey = request.getHeader("apikey");
		System.out.println(apikey);
		
		X_Consumer_Groups = request.getHeader("X-Consumer-Groups");
		System.out.println(X_Consumer_Groups);
		
		System.out.println("------------HEADERS----------------");
	}

	private String getBody(HttpServletRequest request) throws IOException {
		// TODO Auto-generated method stub
		
		System.out.println("In Request Body");
		
		String body = null;
	    StringBuilder stringBuilder = new StringBuilder();
	    BufferedReader bufferedReader = null;

	    try {
	        InputStream inputStream = request.getInputStream();
	        if (inputStream != null) {
	            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
	            char[] charBuffer = new char[128];
	            int bytesRead = -1;
	            while ((bytesRead = bufferedReader.read(charBuffer)) > 0) {
	                stringBuilder.append(charBuffer, 0, bytesRead);
	            }
	        } else {
	            stringBuilder.append("");
	        }
	    } catch (IOException ex) {
	        throw ex;
	    } finally {
	        if (bufferedReader != null) {
	            try {
	                bufferedReader.close();
	            } catch (IOException ex) {
	                throw ex;
	            }
	        }
	    }

	    body = stringBuilder.toString();
	    return body;
	}

	public static String getAuthorization() {
		return authorization;
	}

	public static void setAuthorization(String authorization) {
		RequestController.authorization = authorization;
	}

	public static String getX_Consumer_Custom_ID() {
		return X_Consumer_Custom_ID;
	}

	public static String getX_Consumer_Username() {
		return X_Consumer_Username;
	}

	public static String getApikey() {
		return apikey;
	}

	public static String getX_Consumer_Groups() {
		return X_Consumer_Groups;
	}

	public static String getBody() {
		return body;
	}

	

}