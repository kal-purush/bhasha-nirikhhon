
public class Car extends Vehicle {
	
	private boolean roofRackorNot;
	private int repair = 400;
	
	
	public boolean isRoofRackorNot() {
		return roofRackorNot;
	}


	public void setRoofRackorNot(boolean roofRackorNot) {
		this.roofRackorNot = roofRackorNot;
	}
	
	public int getRepair() {
		
		return repair;
	}


	public Car(int vehicleID, String make, int numOfWheels, String colour, int numOfSeats, int price, boolean roofRackorNot) {
		super(vehicleID, make, numOfWheels, colour, numOfSeats, price);
		this.roofRackorNot = roofRackorNot;
	}



	

	

	
	
	
	

}