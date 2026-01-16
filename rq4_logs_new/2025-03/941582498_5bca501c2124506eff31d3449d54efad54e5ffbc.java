
import help.StaffMemberHelp;
import helperclass.HelperClass;

import java.util.Scanner;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        StaffMemberHelp.addStaffMember();
        Scanner scanner = new Scanner(System.in);
        while (true) {
            try {
                HelperClass.printMenu();
                HelperClass.printSymbol(56);
                System.out.print(HelperClass.purple + "Choose an option : " + HelperClass.reset);
                int choice = scanner.nextInt();
                if (choice == 0) {
                    System.out.println("Exiting program...");
                    break;
                }

                switch (choice) {
                    case 1:
                        StaffMemberHelp.insertStaffMembers();
                        break;
                    case 2:
                        StaffMemberHelp.upDateStaffMembers();
                        break;
                    case 3:
                        StaffMemberHelp.printPaginationStaffMembers();
                        break;
                    case 4:
                        StaffMemberHelp.removeStaffMembers();
                        break;
                    default:
                        System.out.println("Invalid choice, please try again.");
                }
            } catch (Exception e) {
                System.out.println("Error: Invalid input. Please enter a number.");
                scanner.nextLine();
            }
        }
        scanner.close();
    }
}
