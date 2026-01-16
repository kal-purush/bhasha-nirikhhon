package ru.sandem.java3.lesson6.ClientConsole;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class ClientConsole {

    private static final String SERVER_ADDRES = "localhost";
    private static final int SERVER_PORT = 8080;
    private Socket socket;
    private Scanner scannerServer;
    private Scanner scannerConsole;
    private PrintWriter out;

    public ClientConsole() {

        try {
            socket = new Socket(SERVER_ADDRES, SERVER_PORT);
            scannerServer = new Scanner(socket.getInputStream());
            out = new PrintWriter(socket.getOutputStream());
            scannerConsole = new Scanner(System.in);
        } catch (IOException e) {
//            e.printStackTrace();
            System.out.println("Нет подключения к серверу");
            return;
        }

        System.out.println("Подключено к серверу");

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        if (scannerServer.hasNext()) {
                            String message = scannerServer.nextLine();
                            System.out.println("Server: " + message);
                            if ("end".equalsIgnoreCase(message)) {
                                closing();
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        if (scannerConsole.hasNext()) {
                            String message = scannerConsole.nextLine();
                            sendMessage(message);
                            if ("end".equalsIgnoreCase(message)) {
                                closing();
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                }
            }
        }).start();

    }

    private void closing(){
        try {
            socket.close();
            out.close();
            scannerServer.close();
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
        } finally {
            System.exit(0);
        }
    }

    private void sendMessage(String message) {
        out.println(message);
        out.flush();
    }
}