package in;

import model.User;
import service.AuthService;

import java.util.Scanner;

public class AuthController {
    private AuthService authService;

    public AuthController(AuthService authService) {
        this.authService = authService;
    }

    public void register() {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Login: ");
        String username = scanner.nextLine();
        System.out.print("Password: ");
        String password = scanner.nextLine();

        if (authService.register(username, password)) {
            System.out.println("Register successful.");
        } else {
            System.out.println("Username already in use.");
        }
    }

    public User login() {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Login: ");
        String username = scanner.nextLine();
        System.out.print("Password: ");
        String password = scanner.nextLine();

        User user = authService.login(username, password);
        if (user != null) {
            System.out.println("Login successful.");
            return user;
        } else {
            System.out.println("Wrong login or password.");
            return null;
        }
    }

    public void exportLogs(){
        Scanner scanner = new Scanner(System.in);
        System.out.print("input the Log-data filename: ");
    }
}