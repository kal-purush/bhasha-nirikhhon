package squabbler;

import java.io.Serializable;
import java.util.ArrayList;

public class Business implements Serializable {
	private static final long serialVersionUID = 1174418428708492256L;

	private double rating;
	private String price;
	private boolean is_closed;
	private String name;
	private String url;
	private Coordinates coordinates;
	private Address location;
	private ArrayList<User> interestedUsers;
	private ArrayList<User> groupUsers;

	public Business(int rating, String price, boolean is_closed, String name, String url, Coordinates coordinates,
			Address location) {
		this.rating = rating;
		this.price = price;
		this.is_closed = is_closed;
		this.name = name;
		this.url = url;
		this.coordinates = coordinates;
		this.location = location;
	}

	public double getRating() {
		return rating;
	}

	public String getPrice() {
		return price;
	}

	public boolean getClosed() {
		return is_closed;
	}

	public String getName() {
		return name;
	}

	public String getUrl() {
		return url;
	}

	public Coordinates getCoordinates() {
		return coordinates;
	}

	public double getLatitude() {
		return coordinates.latitude;
	}

	public double getLongitude() {
		return coordinates.longitude;
	}

	public String getAddress() {
		String output = "";
		for (String str : location.display_address)
			output += str + " ";
		return output;
	}
	
	public void likesRestaurant(User ie, boolean single) {
		if(interestedUsers == null) interestedUsers = new ArrayList<User>();
		if(single) {
			if(!interestedUsers.contains(ie))
				interestedUsers.add(ie);
		} else {
			groupUsers.add(ie);
		}
	}
	
	//returns whether the user has decided on the restaurant or not	
	public boolean hasDecidedOnRestaurant(User ie) {
		
		return (interestedUsers.contains(ie) || groupUsers.contains(ie));
		
	} 
	
	public ArrayList<User> getInterestedUsers() {
		return this.interestedUsers;
	}

	class Coordinates implements Serializable {
		private static final long serialVersionUID = 1178708492256L;
		private double latitude;
		private double longitude;
	}

	class Address implements Serializable {
		private static final long serialVersionUID = 117884256L;
		private String[] display_address;
	}
	
	
}