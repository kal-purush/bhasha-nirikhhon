package seng401Proj;



import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;


import org.json.JSONObject;


import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

public class Images {

	public static Response getImages(@Context UriInfo info) throws UnirestException{

		String action = info.getQueryParameters().getFirst("action");
		String image = info.getQueryParameters().getFirst("image");
		
		System.out.println("action " + action);
		System.out.println("image " + image);
		
		JSONObject jsonObject = new JSONObject();
		
		String restCall = "https://api.auroras.live/v1/?type=images";
		if (action != null){
			if (action.equals("list")){	
				System.out.println("Got in the action place");
				restCall = restCall + "&action=" + action;
				
			}
		}
		
		
		if (image != null){
			System.out.println("Got in the image place");
			restCall = restCall + "&image=" + image;
			
			HttpResponse<java.io.InputStream> response = Unirest.get(restCall).asBinary();
			return Response.status(200).entity(response.getBody()).type("image/png").build();
			

		}
		
		HttpResponse<JsonNode> response = Unirest.get(restCall).asJson();
				
		jsonObject = response.getBody().getObject();
		String att = "Powered by Auroras.live";
		jsonObject.put("Attribution", att);
		return Response.status(200).entity(response.getBody().toString()).build();

	}
}