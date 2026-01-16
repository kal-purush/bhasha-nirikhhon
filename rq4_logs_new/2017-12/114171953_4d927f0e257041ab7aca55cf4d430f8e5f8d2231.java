package airkrista;

import java.util.*;

public class AirKrista {

    public static void main(String args[]) {
	Scanner keyboard = new Scanner(System.in);
	int input = 0;

	while (true) {
	    System.out.println("************ MAIN MENU ************");
	    System.out.println("1. Update Database");
	    System.out.println("2. Display Arrivals");
	    System.out.println("3. Display Departures");
	    System.out.println("4. Display Air Canada Flights");
	    System.out.println("5. Purchase Tickets");
	    System.out.println("6. Refund Tickets");
	    System.out.println("7. Logoff");

	    input = keyboard.nextInt();

	    switch (input) {
		case 1:
		    updateDatabase();
		    break;

		case 2:
		    displayArrivals();
		    break;

		case 3:
		    displayDepartures();
		    break;

		case 4:
		    displayAirCanada();
		    break;

		case 5:
		    purchaseTickets();
		    break;

		case 6:
		    refundTickets();
		    break;

		case 7:
		    logoff();
		    break;

		default:
		    System.out.println("Invalid menu entry!");
		    break;
	    }
	}
    }

    // Complete the folllowing methods
    public static void updateDatabase() {

    }

    public static void displayArrivals() {

    }

    public static void displayDepartures() {

    }

    public static void displayAirCanada() {

    }

    public static void purchaseTickets() {

    }

    public static void refundTickets() {

    }

    public static void logoff() {

    }

}