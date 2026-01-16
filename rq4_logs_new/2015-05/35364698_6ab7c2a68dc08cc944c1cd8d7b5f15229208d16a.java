package com.company;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

/**
 * The basic TCPServer class
 */
public class BasicTCPServer implements DataEvent {
    ArrayList<Connection> connections;
    ServerEvent serverEvent;
    Thread serverThread;
    int port;
    int numThreads;
    TCPServer server;

    public BasicTCPServer(ServerEvent se, int port, int numberOfThreads) {
        this.serverEvent = se;
        this.port = port;
        this.numThreads = numberOfThreads;
        connections = new ArrayList<Connection>();
        this.server = new TCPServer(this, port, numberOfThreads, connections);

    }


    /**
     *
     */
    public void start(){
        serverThread = new Thread(server);
        serverThread.start();
    }

    /**
     *
     */
    public void stop() throws InterruptedException{
        server.stop();
        serverThread.join();

    }

    /**
     *
     */
    @Override
    public void receiveData (Connection u, String s) {
        //Log here
        serverEvent.receive(u, s);
    }

    /**
     *
     */
    public void send(Connection connection, String message) {
        try{
            PrintWriter output = new PrintWriter(connection.connection.getOutputStream(), true);
            output.println(message);
        } catch(IOException ex){
            System.err.println(ex);
        }
    }

    public void sendAll(String msg){
        try{
            PrintWriter output;
            for(Connection u : connections){
                output = new PrintWriter(u.connection.getOutputStream(), true);
                output.println(msg);
            }
        } catch(IOException ex){
            System.err.println(ex);
        }
    }

}