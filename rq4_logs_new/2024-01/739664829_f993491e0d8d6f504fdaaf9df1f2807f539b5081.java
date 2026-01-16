package busbookingapp;

import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.Scanner;

class Passenger {
    private String name;
    private String phoneNumber;

    public Passenger(String name, String phoneNumber) {
        this.name = name;
        this.phoneNumber = phoneNumber;
    }

    public String getName() {
        return name;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }
}

class Bus {
    private int busNumber;
    private String busOperator;
    private String destination;
    private int capacity;
    private int bookedSeats;
    private ArrayList<Passenger> passengers;

    public Bus(int busNumber, String busOperator, String destination, int capacity) {
        this.busNumber = busNumber;
        this.busOperator = busOperator;
        this.destination = destination;
        this.capacity = capacity;
        this.bookedSeats = 0;
        this.passengers = new ArrayList<>(capacity);
    }

    public int getBusNumber() {
        return busNumber;
    }

    public void setBusNumber(int busNumber) {
        this.busNumber = busNumber;
    }

    public String getBusOperator() {
        return busOperator;
    }

    public void setBusOperator(String busOperator) {
        this.busOperator = busOperator;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public int getCapacity() {
        return capacity;
    }

    public int getBookedSeats() {
        return bookedSeats;
    }

    public boolean checkAvailability(int seats) {
        return capacity - bookedSeats >= seats;
    }

    public void bookSeats(int seats, ArrayList<Passenger> passengers) {
        if (checkAvailability(seats)) {
            bookedSeats += seats;
            this.passengers.addAll(passengers.subList(0, seats));
            System.out.println("Seats booked successfully for " + seats + " passengers");
        } else {
            System.out.println("Sorry, seats are not available");
        }
    }

    public void displayAvailableSeats() {
        System.out.println("Available Seats in Bus " + busNumber + ": " + (capacity - bookedSeats));
    }
}

class BusReservation {
    private ArrayList<Bus> buses = new ArrayList<>();

    public void addBus(int busNumber, String busOperator, String destination, int capacity) {
        buses.add(new Bus(busNumber, busOperator, destination, capacity));
        System.out.println("Bus " + busNumber + " added successfully");
    }

    public void displayBuses() {
        if (buses.isEmpty()) {
            System.out.println("No buses found");
            return;
        }
        System.out.println("Bus Number\tBus Operator\tDestination\tCapacity\tBooked Seats\tAvailable Seats");
        for (Bus bus : buses) {
            System.out.println(bus.getBusNumber() + "\t" + bus.getBusOperator() + "\t" +
                    bus.getDestination() + "\t" +
                    bus.getCapacity() + "\t" + bus.getBookedSeats() + "\t" + (bus.getCapacity() - bus.getBookedSeats()));
        }
    }

    public void bookBus(int busNumber, int seats, ArrayList<Passenger> passengers) {
        Bus bus = getBusByNumber(busNumber);
        if (bus == null) {
            System.out.println("Bus " + busNumber + " not found");
            return;
        }
        bus.bookSeats(seats, passengers);
        bus.displayAvailableSeats();
    }

    private Bus getBusByNumber(int busNumber) {
        for (Bus bus : buses) {
            if (bus.getBusNumber() == busNumber) {
                return bus;
            }
        }
        return null;
    }
}

public class BusReservationSystem {
    public static void main(String[] args) {
        BusReservation busReservation = new BusReservation();
        Scanner scanner = new Scanner(System.in);

        boolean done = false;
        while (!done) {
            System.out.println("Bus Reservation System Menu:");
            System.out.println("___________________________________________________________________________________________________________________________________________");
            System.out.println("\n1. Add a bus");
            System.out.println("2. Display all buses");
            System.out.println("3. Book seats on a bus");
            System.out.println("4. Exit");
            System.out.println("___________________________________________________________________________________________________________________________________________");
            System.out.print("\nEnter your choice (1-4): ");
            int choice = getUserChoice(scanner);
            scanner.nextLine(); // Consume newline character

            switch (choice) {
                case 1:
                    System.out.print("Enter bus number: ");
                    int busNumber = scanner.nextInt();
                    System.out.print("Enter bus operator: ");
                    String busOperator = scanner.next();
                    System.out.print("Enter destination: ");
                    String destination = scanner.next();
                    System.out.print("Enter capacity: ");
                    int capacity = scanner.nextInt();
                    busReservation.addBus(busNumber, busOperator, destination, capacity);
                    break;
                case 2:
                    busReservation.displayBuses();
                    break;
                case 3:
                    System.out.print("Enter bus number: ");
                    busNumber = scanner.nextInt();
                    System.out.print("Enter number of seats to book: ");
                    int seats = scanner.nextInt();

                    // Collect passenger details
                    ArrayList<Passenger> passengers = new ArrayList<>();
                    for (int i = 0; i < seats; i++) {
                        scanner.nextLine(); // Consume newline character
                        System.out.print("Enter passenger name: ");
                        String name = scanner.nextLine();
                        System.out.print("Enter passenger phone number: ");
                        String phoneNumber = scanner.nextLine();
                        passengers.add(new Passenger(name, phoneNumber));
                    }

                    busReservation.bookBus(busNumber, seats, passengers);
                    break;
                case 4:
                    done = true;
                    break;
                default:
                    System.out.println("Invalid choice");
                    break;
            }
        }

        scanner.close();
    }
 private static int getUserChoice(Scanner scanner) {
    int choice = -1;
    boolean validInput = false;

    while (!validInput) {
        try {
            choice = scanner.nextInt();
            scanner.nextLine(); // Consume newline character
            validInput = true;
        } catch (InputMismatchException e) {
            System.out.println("Invalid input. Please enter a number.");
            scanner.nextLine(); 
        }
    }

    return choice;
}
}