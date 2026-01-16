package seng401Proj;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam; 
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.*;

import org.json.JSONException; 
import org.json.JSONObject;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;


public class Ace{
	
	
public static Response getAce(@Context UriInfo info)throws JSONException, UnirestException{
	//Optional Paramaters Lat and Long are only required when data = Probability
	// Should always check for lat and long though.
	String data = info.getQueryParameters().getFirst("data");
	String lattitude = info.getQueryParameters().getFirst("lat");
	String longitude = info.getQueryParameters().getFirst("long");
	
	
	
        	JSONObject jsonObject = new JSONObject();
    		HttpResponse<JsonNode> response = Unirest.get("https://api.auroras.live/v1/?type=ace&data="+data+"&lat="+lattitude+"&long="+longitude)
    										.asJson();
    		               jsonObject = response.getBody().getObject();
    		String att = "Powered by Auroras.live";
    		jsonObject.put("Attribution", att);
    		

        return Response.status(200).entity(response.getBody().toString()).build();
	}
}
