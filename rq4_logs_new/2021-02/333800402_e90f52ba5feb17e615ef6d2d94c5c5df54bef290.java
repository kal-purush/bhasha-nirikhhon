package JAVA_LEVEL_2.Java_lvl2_homework.git.lesson6.hw6;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

public class Client {
    public static void main(String[] args) { myClient();
    }
    public static void myClient(){
        try (
                Socket socket = new Socket("localhost",8180);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream());
                Scanner scanner = new Scanner(System.in)) {
            String message = "";
            Thread clientThread = new Thread(() -> {
                String serverMessage = "";
                try {
                    while (!socket.isClosed()) {
                        serverMessage = in.readLine();
                        System.out.println(serverMessage);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            clientThread.start();
            do {
                message = scanner.nextLine();
                out.println(message);
                out.flush();
            } while (!message.equalsIgnoreCase("stop"));
        } catch (IOException e) {
                e.printStackTrace();
        }
    }
}