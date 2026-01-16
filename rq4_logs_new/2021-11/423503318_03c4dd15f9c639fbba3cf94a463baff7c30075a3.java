package com.socketprog;
import java.net.*;
import java.io.*;
import java.util.*;
public class Client {
    public static void main(String args[])throws IOException{

        // instantiate a socket for client to connect to server at port 22000
        Socket client = new Socket("127.0.0.1",22000);

        // create input and output streams for a client to connect to server
        DataInputStream is = new DataInputStream(client.getInputStream());
        DataOutputStream os = new DataOutputStream(client.getOutputStream());


        while (true){
            // create scanner variable to receive inputs from client through terminal
            Scanner sc = new Scanner(System.in);

            // receive server msg and print it in client terminal to see the communication
            String server_msg = is.readUTF();
            System.out.println("[SERVER] : "+server_msg);

            String user_input = sc.nextLine();

            if (user_input.equals("NO")){
                os.writeUTF(user_input);
                System.out.println(is.readUTF());
                break;
            }
            os.writeUTF(user_input);
        }
        is.close();
        os.close();
        client.close();
    }
}