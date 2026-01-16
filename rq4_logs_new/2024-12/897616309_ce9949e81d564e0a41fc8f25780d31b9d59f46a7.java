// testing API on the console.

package com.java.codeFront;
import com.java.codeFront.controller.NewsController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Scanner;

@Component
public class ConsoleRunner implements CommandLineRunner {

    @Autowired
    private NewsController newsController;

    @Override
    public void run(String... args) throws Exception {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Welcome to the News API Console!");
        System.out.println("Type 'exit' to quit.");

        while (true) {
            System.out.println("\nEnter a topic to search for news (e.g., 'technology'):");
            String query = scanner.nextLine();

            if ("exit".equalsIgnoreCase(query)) {
                System.out.println("Goodbye!");
                break;
            }

            try {
                String news = newsController.getNewsFromConsole(query);
                System.out.println("\nFetched News:");
                System.out.println(news);
            } catch (Exception e) {
                System.out.println("An error occurred: " + e.getMessage());
            }
        }
    }
}