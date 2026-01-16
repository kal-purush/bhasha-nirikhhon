package berlin.vs.u1clientserver.client;

import java.io.*;
import java.io.BufferedReader;import java.io.DataOutputStream;import java.io.InputStreamReader;import java.lang.Exception;import java.lang.String;import java.lang.System;import java.net.*;import java.net.Socket;

class TCPClient {
    public static void main(String argv[]) {
        String sentence;
        String modifiedSentence;

        try {
            BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
            Socket clientSocket = null;


            clientSocket = new Socket("localhost", 6789); // via Oberfläche eingeben

            DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
            BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

            sentence = inFromUser.readLine();
            outToServer.writeBytes(sentence + '\n');

            modifiedSentence = inFromServer.readLine();
            System.out.println("FROM SERVER: " + modifiedSentence);

            clientSocket.close();
        } catch (Exception ex) {
            System.out.print("Fehler beim Versuch eine Verbindung zum Server aufzubauen: " + ex.getMessage());
        }
    }
}