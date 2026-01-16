package session3.assignment3;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Room rm = new Room();
        Scanner sc = new Scanner(System.in);
        while (true) {
            System.out.println("Application Manager Candidate");
            System.out.println("Enter 1: To insert person for rent");
            System.out.println("Enter 2: To remove person");
            System.out.println("Enter 3: To show infor");
            System.out.println("Enter 4: To exit:");
            String line = sc.nextLine();
            switch (line) {
                case "1": {
                    System.out.print("Enter name: ");
                    String name = sc.nextLine();
                    System.out.print("Enter age: ");
                    int age = sc.nextInt();
                    System.out.print("Enter email: ");
                    String email = sc.nextLine();
                    System.out.print("Enter phone: ");
                    String phone = sc.nextLine();
                    System.out.print("Enter price: ");
                    double price = sc.nextDouble();

                    User user = new User(name, age, email, phone, price);
                    rm.add(user);
                    sc.nextLine();
                }
                case "2": {
                    rm.delete();
                    break;
                }
                case "3": {
                    rm.show();
                    break;
                }
                case "4": {
                    return;
                }
                default:
                    System.out.println("Invalid");
                    continue;

            }
        }
    }
}