package com.distributedSystems.ReplicationJarProject.BackUpCoordinator;

import com.distributedSystems.ReplicationJarProject.Producer.BackUpRequest;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class BackUpCoordinator {


    private ServerSocket serverSocket = null;


    public BackUpCoordinator(){


    }


    public static void main(String[] args) {

        new BackUpCoordinator().startServer(4444);
    }



    public void startServer(int port) {

        try {
            System.out.println("SERVIDOR ESPERANDO CLIENTES EN PUERTO: "+port);
            serverSocket = new ServerSocket(port);
            while (true){

                Socket clientSocket = serverSocket.accept();
                BackUpCoordinatorClientHandler clientHandler= new BackUpCoordinatorClientHandler(clientSocket);
                clientHandler.start();


            }

        } catch (IOException e) {
            System.out.println("Error: "+e);
            e.printStackTrace();
        }

    }



    private class BackUpCoordinatorClientHandler extends Thread {
        private Socket clientSocket;
        public ObjectInputStream in;
        public ObjectOutputStream out;

        public BackUpCoordinatorClientHandler(Socket socket) {
            this.clientSocket = socket;
            System.out.println("Server accepted new client, Thread: "+Thread.currentThread().getId());

        }

        public void run() {



            try {
                System.out.println("Server accepted new client, Thread: "+Thread.currentThread().getId());
                in = new ObjectInputStream(clientSocket.getInputStream());
                out = new ObjectOutputStream(clientSocket.getOutputStream());


                Object request =  in.readObject(); //Receives a request
                System.out.println("REQUEST NAME: "+ request.getClass().getSimpleName());
                synchronized (this) { //A process is either filling the stores or taking an ingredient from them, not both
                    switch (request.getClass().getSimpleName()) {
                        case "BackUp": //The seller is filling the stores
                            /**Seller Fill Store**/
                            BackUpRequest backUpRequest = (BackUpRequest) request;
                            System.out.println("BackUp request received: " + backUpRequest);
                                //todo manejar logica de negocio del backup
                            System.out.println("BackUp request fulfilled: ");

                            break;
                        case "VoteRequest": //The smoker is taking an item

                            VoteRequest voteRequest = (VoteRequest) request;
                            System.out.println("Vote Request received: " + voteRequest);

                           //todo manejar logica de votos para el global commit

                            //log("Ingredient Request response: " + response);

                            break;
                        default:
                            System.out.println("Request not recognized, request received: " + request.getClass().getSimpleName());
                            //log("Server received an unrecognized Request : " + request);
                            break;
                    }

                    in.close();
                    out.close();
                    clientSocket.close();
                    System.out.println("Server closed socket for request:"+request+", Thread: "+Thread.currentThread().getId());
                    //log("Server closed socket for request:"+request+", Thread: "+Thread.currentThread().getId());
                }




            } catch (IOException
                    | ClassNotFoundException
                    e) {
                e.printStackTrace();
            }
        }
    }
}