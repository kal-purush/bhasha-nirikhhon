
import java.util.Scanner;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class HallBookingSystem {
    private static String[][] seatTable;
    private static String AVAILABLE = "[AV]";
    private static StringBuilder history;
    private static StringBuilder bookingHistory = new StringBuilder();

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.println();
        System.out.println("\t\t\t\t=========== HALL BOOKING SYSTEM ===========");
        System.out.print("\t\t\t\t-> Please enter the number of rows of seats: ");
        int row = scanner.nextInt();
        System.out.print("\t\t\t\t-> Please enter the number of columns of seats: ");
        int column = scanner.nextInt();

        seatTable = new String[row][column];

        for (int i = 0; i < row; i++) {
            for (int j = 0; j < column; j++) {
                seatTable[i][j] = AVAILABLE;
            }
        }

        while (true) {
            System.out.println("\t\t\t\t=========== HALL BOOKING SYSTEM ===========");
            System.out.println("\t\t\t\t<A>. Hall Booking");
            System.out.println("\t\t\t\t<B>. Hall Checking");
            System.out.println("\t\t\t\t<C>. Showtime Checking");
            System.out.println("\t\t\t\t<D>. Booking History");
            System.out.println("\t\t\t\t<E>. Rebooting Hall");
            System.out.println("\t\t\t\t<F>. Exit");
            System.out.print("\t\t\t\t=> Enter your choice: ");
            char choice = Character.toUpperCase(scanner.next().charAt(0));

            switch (choice) {
                case 'A':
                    performBooking(scanner);
                    break;
                case 'B':
                    performCheckingHall(scanner);
                    break;
                case 'C':
                    performShowtimeChecking(scanner);
                    break;
                case 'D':
                    performBookingHistory();
                    break;
                case 'E':
                    performRebooting();
                    break;
                case 'F':
                    System.out.println("\t\t\t\t(=) Exiting...");
                    return;
                default:
                    System.out.println("\t\t\t\t Invalid choice!!!. Please try again.");
            }
        }
    }


    private static String getSeatLabel(int row, int column) {
        char label = (char) ('A' + row);
        return label + "-" + (column + 1);
    }


    private static void performBooking(Scanner scanner) {

        System.out.println("\t\t\t\t=========== PERFORM BOOKING SYSTEM ===========");
        System.out.println("\t\t\t\t<<= Start Booking Process =>>");
        System.out.println("\t\t\t\t\tShowtime Information");
        System.out.println();
        System.out.println("\t\t\t\t(A). Morning (10:00AM - 12:30PM)");
        System.out.println("\t\t\t\t(B). Afternoon (03:00PM - 5:30PM)");
        System.out.println("\t\t\t\t(C). Night (07:00AM - 09:30PM)");
        System.out.print("\t\t\t\t  ******************************\n");
        System.out.println("\t\t\t\t* For Single eg. C-1        *");
        System.out.println("\t\t\t\t* For Multiples eg. C-1,C-2 *");
        System.out.print("\t\t\t\t  *****************************\n");
        System.out.print("\t\t\t\t      (=>) Please select show times: ");

        char bookingTime = Character.toUpperCase(scanner.next().charAt(0));
        String hall;
        switch (bookingTime) {
            case 'A':
                timeMorning();
                hall = "Morning";
                break;
            case 'B':
                timeAfternoon();
                hall = "Afternoon";
                break;
            case 'C':
                timeNight();
                hall = "Night";
                break;
            default:
                System.out.println("\t\t\t\t Invalid choice!!!. Please try again.");
                return;
        }
        bookSeats(scanner, hall, String.valueOf(bookingTime));
    }


    private static void timeNight() {

        System.out.println("\t\t\t\t#HALL - Night");
        System.out.println("\t\t\t\t| A-1 :: AV |     | A-2 :: AV |");
        System.out.println("\t\t\t\t| B-1 :: AV |     | B-2 :: AV |");
    }


    private static void timeAfternoon() {

        System.out.println("\t\t\t\t#HALL - Afternoon");
        System.out.println("\t\t\t\t| A-1 :: AV |     | A-2 :: AV |");
        System.out.println("\t\t\t\t| B-1 :: AV |     | B-2 :: AV |");
    }


    private static void timeMorning() {

        System.out.println("\t\t\t\t#HALL - Morning");
        System.out.println("\t\t\t\t| A-1 :: AV |     | A-2 :: AV |");
        System.out.println("\t\t\t\t| B-1 :: AV |     | B-2 :: AV |");
    }


    private static void storeBookingHistory(String seatInput, String studentID, String hall, String bookingTime) {

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String currentTime = LocalDateTime.now().format(formatter);
        bookingHistory.append("\t\t\t\tHall: ").append(hall).append("\n ");
        bookingHistory.append("\t\t\t\tTime: ").append(currentTime).append("\n ");
        bookingHistory.append("\t\t\t\tSeats: ").append(seatInput).append("\n ");
        bookingHistory.append("\t\t\t\tStudent ID: ").append(studentID).append("\n");
    }


    private static void bookSeats(Scanner scanner, String hall, String bookingTime) {

        System.out.print("\t\t\t\t> Please select available seats: ");
        scanner.nextLine();
        String seatInput = scanner.nextLine().toUpperCase();
        String[] seats = seatInput.split(",");

        System.out.print("\t\t\t\t> Please enter the student ID: ");
        String studentID = scanner.nextLine();
        while (!studentID.matches("\\d+")) {
            System.out.print("\t\t\t\tInvalid input. Please enter a valid student ID (only numbers): ");
            studentID = scanner.nextLine();
        }


        System.out.print("\t\t\t\t> Are you sure to book? (Y/n): ");
        char confirmation = scanner.next().charAt(0);
        if (Character.toUpperCase(confirmation) == 'Y') {
            for (String seat : seats) {
                String[] pos = seat.split("-");
                if (pos.length == 2) {
                    int row = pos[0].charAt(0) - 'A';
                    int column = Integer.parseInt(pos[1]) - 1;
                    if (row >= 0 && row < seatTable.length && column >= 0 && column < seatTable[row].length) {
                        seatTable[row][column] = "[BO]";
                        System.out.println("\t\t\t\t===========================================");
                    }
                }
            }

            // Process the booking
            System.out.println();
            System.out.println("\t\t\t\t=> Booking confirmed for seats: " + seatInput);
            System.out.println("\t\t\t\t=> Student ID: " + studentID);
            System.out.println("\t\t\t\t===========================================");
            storeBookingHistory(seatInput, studentID, hall, bookingTime);
        } else {
            System.out.println("\t\t\t\t=> Booking canceled.");
            System.out.println("\t\t\t\t===========================================");
        }
    }


    private static void performCheckingHall(Scanner scanner) {
        System.out.println("\t\t\t\t============== CHECKING HALL ==============");
        System.out.println("\t\t\t\tShowtimes: ");

        System.out.println("\t\t\t\t#HALL - Morning");
        displaySeatTable();

        System.out.println("\t\t\t\t#HALL - Afternoon");
        displaySeatTable();

        System.out.println("\t\t\t\t#HALL - Night");
        displaySeatTable();
        System.out.println("\t\t\t\t===========================================");
    }


    private static boolean displaySeatTable() {
        for (int i = 0; i < seatTable.length; i++) {
            System.out.print("\t\t\t\t| ");
            for (int j = 0; j < seatTable[i].length; j++) {
                System.out.printf("%-10s", getSeatLabel(i, j) + "::" + seatTable[i][j]);
            }
            System.out.println(" |");
        }
        System.out.println();

        return false;
    }


    private static void performShowtimeChecking(Scanner scanner) {
        System.out.println("\t\t\t\t============== SHOW TIME INFORMATION ==============");
        System.out.println("\t\t\t\tA. Morning (10:00AM - 12:30PM)");
        System.out.println("\t\t\t\tB. Afternoon (03:00PM - 5:30PM)");
        System.out.println("\t\t\t\tC. Night (07:00AM - 09:30PM)");
        System.out.println("\t\t\t\t===================================================");
    }


    private static void performBookingHistory() {
        displayBookingHistory();
    }


    private static void displayBookingHistory() {
        System.out.println("\t\t\t\t============= BOOKING HISTORY =============");
        if (bookingHistory.length() == 0) {
            System.out.println("\t\t\t\t<=> No booking history");
        } else {
            System.out.println();
            System.out.println(bookingHistory.toString());
        }
        System.out.println("\t\t\t\t===========================================");
    }


    private static void performRebooting() {
        for (int i = 0; i < seatTable.length; i++) {
            for (int j = 0; j < seatTable[i].length; j++) {
                seatTable[i][j] = AVAILABLE;
            }
        }

        bookingHistory.setLength(0);
        System.out.println("\t\t\t\t============= HISTORY CLEARED =============");
        System.out.println("\t\t\t\tHall has been successfully rebooted.");
        System.out.println("\t\t\t\tAll seats are now available.");
        System.out.println("\t\t\t\tBooking history has been cleared.");
        System.out.println("\t\t\t\t===========================================");
    }

}