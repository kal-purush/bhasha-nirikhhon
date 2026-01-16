package com.vc.service.controller;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Iterator;

import javax.ws.rs.Produces;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.vc.model.NearByResponse;
import com.vc.model.User;

@RestController
public class VacationPlannerNearByService {
	
	  @GetMapping(path={"/NearBy"})
	  @Produces(value = "application/json")
	  @ResponseBody()
	  public String getNearByRestaurants(@RequestParam("keyword") String keyword,@RequestParam("location") String location, @RequestParam("radius") String radius, @RequestParam("type") String type )
	  {
		    String url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?" 
		    				+ "location=" + location 
		    				+ "&type=" + type
		    				+"&radius=" + radius
		    				+"&keyword" + keyword
		    				+"&key=AIzaSyC4x3E86QCk1dW2iVA5GHmRH9mNKtZx-1g";
			URL obj;
			try {
				obj = new URL(url);
				HttpURLConnection con = (HttpURLConnection) obj.openConnection();
				con.setRequestMethod("GET");
				int responseCode = con.getResponseCode();
				if(responseCode == 200)
				{
					BufferedReader br = new BufferedReader(new InputStreamReader(
							(con.getInputStream())));
						JSONParser parser = new JSONParser();
						JSONObject jsonObj = (JSONObject) parser.parse(br);
						JSONArray  results =  (JSONArray) jsonObj.get("results");
						con.disconnect();
						return results.toJSONString();
						/*Iterator i = results.iterator();
						 while (i.hasNext()) {
							 JSONObject innerObj = (JSONObject) i.next();
							 NearByResponse response = 
							 System.out.println("===========================================Restaurant Info=======================================");
							 System.out.println("Name   ======>  "  + innerObj.get("name"));
							 System.out.println("rating   ======>  "  + innerObj.get("rating"));
							 JSONObject geometry = (JSONObject) innerObj.get("geometry");
							 JSONObject reslocation = (JSONObject) geometry.get("location");
							 System.out.println("location   ======>  "  + location.get("lat"));
							 System.out.println("location   ======>  "  + location.get("lng"));
						 }*/
						 

						
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				
			}
			return null;
			
	  }

}