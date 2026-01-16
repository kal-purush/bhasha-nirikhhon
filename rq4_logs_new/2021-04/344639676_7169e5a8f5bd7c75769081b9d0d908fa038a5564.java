package com.parkit.parkingsystem;

import static org.assertj.core.api.Assertions.assertThat;


import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.parkit.parkingsystem.constants.ParkingType;
import com.parkit.parkingsystem.dao.ParkingSpotDAO;
import com.parkit.parkingsystem.dao.TicketDAO;
import com.parkit.parkingsystem.integration.config.DataBaseTestConfig;
import com.parkit.parkingsystem.model.ParkingSpot;
import com.parkit.parkingsystem.model.Ticket;

class TicketDAOTest {
	
	private static DataBaseTestConfig databaseTestConfig = new DataBaseTestConfig();
	private static TicketDAO ticketDAO;
	private static ParkingSpotDAO parkingSpotDAO;
	
	@BeforeAll
	public static void setUpClass() throws Exception {
		ticketDAO = new TicketDAO();
		ticketDAO.dataBaseConfig = databaseTestConfig;
		parkingSpotDAO = new ParkingSpotDAO();
	}
	
	@Test
	public void isReturningUser() throws Exception {
		Ticket ticket = new Ticket();
		ticket.setVehicleRegNumber("ABCDEF");
		boolean isBack = ticketDAO.returningUser(ticket.getVehicleRegNumber());
		assertThat(isBack).isTrue();
		
	}
	
	@Test
	public void updateParkingCar() throws Exception {
		ParkingSpot parkingSpot= new ParkingSpot(1, ParkingType.CAR,false);
		boolean isUpdated = parkingSpotDAO.updateParking(parkingSpot);
		assertThat(isUpdated).isTrue();
	}
	
	@Test
	public void updateParkingBike() throws Exception {
		ParkingSpot parkingSpot = new ParkingSpot(1, ParkingType.BIKE,false);
		boolean isUpdated = parkingSpotDAO.updateParking(parkingSpot);
		assertThat(isUpdated).isTrue();
	}
	
	@Test
	public void isSearchingForNewParkingSpot() throws Exception {
		int getNewSlot = parkingSpotDAO.getNextAvailableSlot(ParkingType.CAR);
		assertThat(getNewSlot).isEqualTo(1);
		
		
	}

}