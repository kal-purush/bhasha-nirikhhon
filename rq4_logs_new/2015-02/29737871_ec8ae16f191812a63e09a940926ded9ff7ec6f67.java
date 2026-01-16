package com.qa.flightsearch;

public class AirportSearch {
	
    public static void main(String args[]) {
    	
	
	//			curl -v  -X GET
	//	Long			+ "-0.453/
//    	Lat				+	51.469/
//    	Radius (mi)		+	40?"
	//					+ "appId=208593e7&"
	//					+ "appKey=24ff9976068fc000c241363fd8434a17";
	//	
    	String baseSearch = "https://api.flightstats.com/flex/airports/rest/v1/json/withinRadius/";
		String airportQuery;
		Double originlat = null;
		Double originlong =null;
		Double destinationlat = null;
		Double destinationlong = null;
		Integer radius = 50;
		String originString;
		String destString;
		String appId= "appId=208593e7&";
		String appKey="appKey=24ff9976068fc000c241363fd8434a17";
		
		originString = ""+originlat+"/"+originlong+"/";
		destString =""+destinationlat+"/"+destinationlong+"/";
		airportQuery=baseSearch+""+originString+radius+"?"+appId+appKey;
		}
}