package view;

import controller.PassengerController;
import model.Passenger;

import java.util.Scanner;

public class PassengerView {

    PassengerController passengerController = new PassengerController();
    Scanner scanner = new Scanner(System.in);
    
    public void showPassengerMenu() {
        System.out.println("Passenger");
        System.out.println("1 - Create Passenger");
        System.out.println("2 - Find a Passenger");
        System.out.println("3 - View all Passenger");



        Integer choice = -1;

        while (true) {
            choice = scanner.nextInt();
            switch (choice) {
                case 1:
                    createPassengerMenu();
                    break;
                case 2:
                    //findPassengerMenu();
                case 3:
                    //getAllPassengers();

            }
        }
    }

    private void createPassengerMenu() {
      Passenger passenger = new Passenger();

        System.out.println("Crete Book");

        while (passenger.getId() == null) {
            System.out.println("Enter book id");
            Long id = scanner.nextLong();
            if (passengerController.getById(id).getId() != null) {
                System.out.println("Такая книга уже есть");
            } else {
               passenger.setId(id);
            }
        }

        System.out.println("Enter passenger ");
        while (passenger.getFirstNamePassenger() == null || passenger.getFirstNamePassenger().isBlank()){
            passenger.setFirstNamePassenger(scanner.nextLine());
        }

        while (passenger.getSecondNamePassenger() == null || passenger.getSecondNamePassenger().isBlank()){
            System.out.println("Enter ");
            passenger.setSecondNamePassenger(scanner.nextLine());
        }

        while (passenger.getIdTicket()==null){
            System.out.println("ticket id");
            Long ticketId = scanner.nextLong();
            if(true){//мы должны проверить что билет с таким id есть в базе ticketController.getEntityById(ticketId).getId();
                System.out.println("Билет с таким id не существует");
            }else {
                passenger.setIdTicket(ticketId);
            }
        }

        passengerController.save(passenger);


    }


}