package ua.ithillel.lesson24.Example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class EchoClient {

    public static void main(String[] args) throws IOException {
        Scanner scanner = new Scanner(System.in);

        try(Socket echoSocket = new Socket("localhost", 8080);

        var out = new PrintWriter(echoSocket.getOutputStream(), true);
        var in = new BufferedReader(new InputStreamReader(echoSocket.getInputStream()))){

            String userInput;
            while ((userInput = scanner.nextLine()) != null){
                out.println(userInput);
                if(userInput.equals("exit")){
                    break;
                } else {
                    System.out.println("userInput=" + in.readLine());
                }
            }
        }

    }
}