package Reservation.Hotel_Reservation_Problem;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class HotelReservationTest 
{
	static HotelReservation hotelReservation;
	@BeforeClass
	public static void initialize() {
		hotelReservation = new HotelReservation();
	}
	
	// testing for cheap hotel	
	 @Test
     public void Test()throws Exception {
         HotelReservation hotelReservation = new HotelReservation();
         hotelReservation.HotelDetails();
         String cheapHotel = hotelReservation.CheapestHotel("10Sep2020", "11Sep2020");
         Assert.assertEquals("Lakewood", cheapHotel);
     }
}