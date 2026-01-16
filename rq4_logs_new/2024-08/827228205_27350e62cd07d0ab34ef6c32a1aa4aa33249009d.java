package ru.otus.java.basic.safarov.homework13.client;

import java.net.Socket;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        String input = scanner.nextLine();

        try (Socket socket = new Socket("localhost", 8888);
             PingClient client = new PingClient(socket)) {
            client.send(input);
            System.out.println(client.read());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}