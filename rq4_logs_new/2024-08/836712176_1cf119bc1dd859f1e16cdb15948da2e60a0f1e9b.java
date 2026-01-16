package org.example;

import org.example.model.User;
import org.example.model.UserRole;
import org.example.in.*;
import org.example.repository.AuditRepository;
import org.example.repository.*;
import org.example.service.*;

import java.util.Scanner;

/**
 * The main class
 */
public class Main {
    public static void main(String[] args) {
        UserRepository userRepository = new UserRepository();
        CarRepository carRepository = new CarRepository();
        OrderRepository orderRepository = new OrderRepository();
        AuditRepository auditRepository = new AuditRepository();

        AuditService auditService = new AuditService(auditRepository);
        AuthService authService = new AuthService(userRepository,auditService);
        UserService userService = new UserService(userRepository);
        CarService carService = new CarService(carRepository,auditService,authService);
        SearchService searchService = new SearchService(carRepository,orderRepository,userRepository, auditRepository);
        OrderService orderService = new OrderService(orderRepository, carRepository, userRepository, auditService);

        AuthController authController = new AuthController(authService);
        UserController userController = new UserController(userService);
        CarController carController = new CarController(carService);
        SearchController searchController = new SearchController(searchService, auditService);
        OrderController orderController = new OrderController(orderService);

        Scanner scanner = new Scanner(System.in);
        /**
         * Create a root admin user, set login and password for root Admin
         */
        authService.registerAdmin("root", "root");
        User loggedInUser = null;

        while (true) {
            MainMenuController.mainMenu();
            int choice = scanner.nextInt();
            scanner.nextLine(); // consume newline

            if (choice == 1) {
                authController.register();
            } else if (choice == 2) {
                loggedInUser = authController.login();
                    if (loggedInUser != null) {
                        if (loggedInUser.getRole() == UserRole.ADMIN) {
                            MainMenuController.adminMenu(scanner, userController, loggedInUser, carController, orderController, searchController, authController);
                        } else if (loggedInUser.getRole() == UserRole.MANAGER) {
                            MainMenuController.managerMenu(scanner, carController, orderController, searchController);
                        } else if (loggedInUser.getRole() == UserRole.CUSTOMER) {
                            MainMenuController.customerMenu(scanner, carController, orderController, loggedInUser, searchController);
                        }
                    }
                }
             else if(choice == 3) {
                System.out.print("Bye!");
                break;
            }
        }
    }

}