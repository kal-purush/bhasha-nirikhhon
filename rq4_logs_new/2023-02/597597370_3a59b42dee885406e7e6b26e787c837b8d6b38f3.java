package Network.Client;

import java.io.*;
import java.net.*;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class Client {
    public static void main(String[] args) {

        // Default host and port values
        String host = "localhost";
        int port = 5050;

        // Too many args, exit program
        if(args.length > 2) {
            System.out.println("Too many args: optional args are [host] [port]");
            System.exit(0);
        }

        // If a host was specified, set it 
        if(args.length >= 1) {
            host = args[0];
        }

        // If a port was specified, use it
        if(args.length == 2) {
            try {
                port = Integer.parseInt(args[1]);
            } catch(Exception e) {
                System.out.println("Invalid port value, using default port 5050");
            }
        }

        // Tell the client where it's connecting
        System.out.println("\nConnecting to " + host + ":" + port);
        
        try (Socket s = new Socket(host, port)) {

            // Initialize all of the objects needed to communicate with server
            // Write to server
            PrintWriter pr = new PrintWriter(s.getOutputStream());
            Scanner sc = new Scanner(System.in);

            // Read from server
            InputStreamReader in = new InputStreamReader(s.getInputStream());
            BufferedReader br = new BufferedReader(in);

            // Learn where in the order this client plays
            int turnNum = br.read();
            System.out.println("\nYou are player : " + turnNum);
            System.out.println("The host is player : " + (turnNum == 1 ? 2 : 1));

            // Wait 2 seconds to let spectators register which player is who
            try {
                // Perform the wait
                TimeUnit.SECONDS.sleep(2);
            } 
            
            // Should never trigger
            catch (InterruptedException e) {
                e.printStackTrace();
            }

            // Is true when the game ends
            boolean gameOver = false;

            // Loop until the game is over
            while(!gameOver) {

                // Read and print all of the game state details recieved from host
                String line = null;
                while((line = br.readLine()) != null) {

                    // Encountered if it's the client's turn to play a move
                    if(line.equals("m")) break;

                    // Encountered if the game is over
                    else if(line.equals("END")) {
                        gameOver = true;
                        break;
                    }

                    // Print the output recieved from the host
                    System.out.println(line);
                }

                // If the game is not over, tell the host your move
                if(!gameOver) {
                    pr.write(sc.nextInt());
                    pr.flush();
                    while((line = br.readLine()) != null) {
                        if(line.equals("VALID")) break;
                        System.out.println(line);
                        pr.write(sc.nextInt());
                        pr.flush();
                    }
                }
            }

        } catch (IOException e) {

            // TODO Auto-generated catch block
            System.out.println("Connection to the host was lost!");
        }
    }
    
}