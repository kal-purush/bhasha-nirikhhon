package org.example;

import java.io.*;
import java.net.Socket;
import java.util.Scanner;

public class Client {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        String word = "";
        String exitResponce = "exit";

        try (Socket socket = new Socket("127.0.0.1", 8085);

             BufferedWriter writer =
                     new BufferedWriter(
                             new OutputStreamWriter(
                                     socket.getOutputStream()));

             BufferedReader reader =
                     new BufferedReader(
                             new InputStreamReader(
                                     socket.getInputStream()));



        ) {

            System.out.println("Connected to server");

            while ((word = reader.readLine()) != null) {

                System.out.println(word);

                String response = scanner.nextLine();

                writer.write(response);
                writer.newLine();
                writer.flush();

                if (response.equals(exitResponce)) {
                    System.exit(0);
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}