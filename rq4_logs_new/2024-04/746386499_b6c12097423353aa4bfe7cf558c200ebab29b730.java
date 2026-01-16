package ru.chernov.java.basic.homeworks.homework13;

import java.io.IOException;
import java.net.Socket;
import java.util.Scanner;

public class Client {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            try (Socket clientSocket = new Socket("localhost", 8080)) {
                ExampleClient client = new ExampleClient(clientSocket.getInputStream(), clientSocket.getOutputStream());
                client.read();
                String userMessage = scanner.nextLine();
                if (userMessage.equals("exit")) {
                    break;
                }
                client.send(userMessage);
                client.read();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}