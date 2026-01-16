package ru.otus.homework.hw13.client;

import java.net.Socket;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        try (Socket socket = new Socket("localhost", 8080)) {
            PingClient client = new PingClient(socket.getInputStream(), socket.getOutputStream());
            String info = client.read();
            System.out.println(info);
            String userInput = scanner.nextLine();
            client.send(userInput + "\r\n");
            String result = client.read();
            System.out.println("result: " + result);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}