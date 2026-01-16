package com.company;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketThread {

    ServerSocket serverSocket;
    Socket clientSocket;
    public SocketThread(){

        try {
            serverSocket = new ServerSocket(4444);
            clientSocket = serverSocket.accept();
        }catch (IOException e){
            e.getCause();
        }
    }

    public void createThread(){

        new Thread(() -> {
            try (

                    PrintWriter out =
                            new PrintWriter(clientSocket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(clientSocket.getInputStream()));
            ) {


                String inputLine, outputLine;


                KnockKnockProtocol kkp = new KnockKnockProtocol();
                outputLine = kkp.processInput(null);
                out.println(outputLine);

                while ((inputLine = in.readLine()) != null) {
                    outputLine = kkp.processInput(inputLine);
                    out.println(outputLine);
                    if (outputLine.equals("Bye."))
                        break;
                }


            }catch (IOException e){

            }
        }).start();

    }

}