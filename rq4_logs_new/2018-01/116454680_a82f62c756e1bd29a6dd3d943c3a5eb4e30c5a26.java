package main;

import java.io.IOException;

import com.google.code.geocoder.Geocoder;
import com.google.code.geocoder.GeocoderRequestBuilder;
import com.google.code.geocoder.model.GeocodeResponse;
import com.google.code.geocoder.model.GeocoderRequest;
import com.google.code.geocoder.model.GeocoderResult;
import com.google.code.geocoder.model.GeocoderStatus;
import com.google.code.geocoder.model.LatLng;

public class AddressToGps {
	Float[] coords;
	String regionAddress;
	
	
	public AddressToGps(String address) throws Exception {
		this.regionAddress = address;
		coords = geoCoding(address).clone();
	}

	public Float[] getGps() {
		return coords;
	}
	
	public static Float[] geoCoding(String location) {
		if (location == null)  
			return null;
		
		Geocoder geocoder = new Geocoder();

		GeocoderRequest geocoderRequest = new GeocoderRequestBuilder().setAddress(location).setLanguage("ko").getGeocoderRequest();
		GeocodeResponse geocoderResponse;

		try {
			geocoderResponse = geocoder.geocode(geocoderRequest);
			if (geocoderResponse.getStatus() == GeocoderStatus.OK & !geocoderResponse.getResults().isEmpty()) {
				GeocoderResult geocoderResult=geocoderResponse.getResults().iterator().next();
				LatLng latitudeLongitude = geocoderResult.getGeometry().getLocation();
								  
				Float[] coords = new Float[2];
				coords[0] = latitudeLongitude.getLat().floatValue(); // Lat
				coords[1] = latitudeLongitude.getLng().floatValue(); // Lon
				return coords;
			}	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		return null;
	}
}