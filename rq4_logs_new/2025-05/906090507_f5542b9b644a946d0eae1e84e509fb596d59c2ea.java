package org.dmytr;



import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;


public class Client {

    private static final Logger logger = LogManager.getLogger(Client.class);

    public static void main(String[] args) throws IOException {
        try (Socket socketClient = new Socket("localhost", 8080)) {
            try (PrintWriter printWriter = new PrintWriter(socketClient.getOutputStream(), true);
                 BufferedReader br = new BufferedReader(new InputStreamReader(socketClient.getInputStream()));
                 Scanner scanner = new Scanner(System.in);
            ) {
                new Thread(() -> {
                    String response;
                    try {
                        while ((response = br.readLine()) != null) {
                            System.out.println(response);

                        }

                    } catch (IOException e) {
                        logger.error("[CLIENT] З'єднання з сервером перервано.", e);
                    }
                }).start();

                while (scanner.hasNextLine()) {
                    printWriter.println(scanner.nextLine());
                }


            }
        }


    }
}