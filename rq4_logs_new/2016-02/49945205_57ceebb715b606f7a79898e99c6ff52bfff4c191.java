package klasar;

import java.util.ArrayList;
import java.util.List;

public class TravelAgency {
	private String name;
	private double rating;
	private List<DayTrip> dayTrips;
	private int rateCount;
	
	public TravelAgency(String name) {
		this.name = name;
		
		rating = 0;
		rateCount = 0;
		dayTrips = new ArrayList<DayTrip>();
	}
	
	public DayTrip[] getDayTrips() {
		return (DayTrip[]) dayTrips.toArray();
	}
	
	public String getName(){
		return name;
	}
	
	public double getRating() {
		return rating;
	}
	
	public void rate(int newRating) {
		double a = rating*rateCount + newRating;
		rating = a/(++rateCount);
	}
}