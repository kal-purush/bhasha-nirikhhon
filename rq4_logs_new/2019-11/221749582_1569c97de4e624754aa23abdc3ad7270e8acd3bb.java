package com.developer.socket.app;

import com.developer.socket.thread.*;
import java.net.*;
import java.io.*;

public class ChatClient {

    private String hostname;
    private int port;
    private String userName;
 
    public ChatClient(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }
 
    public void execute() {
        try {

            Socket socket = new Socket(hostname, port);
 
            System.out.printf("\n> Connected to the chat server");
            
            new ReadThread(socket, this).start();
            new WriteThread(socket, this).start();
 
        } catch (UnknownHostException ex) {
            System.out.printf("\nServer not found: " + ex.getMessage());
        } catch (IOException ex) {
            System.out.printf("\nI/O Error: " + ex.getMessage());
        }
    }
 
    public void setUserName(String userName) {
        this.userName = userName;
    }
 
    public String getUserName() {
        return this.userName;
    }
    
    public static void main(String[] args) {
       
        String hostname = "localhost";
        int port = 3000;
        
        ChatClient client = new ChatClient(hostname, port);
        client.execute();
    }
}