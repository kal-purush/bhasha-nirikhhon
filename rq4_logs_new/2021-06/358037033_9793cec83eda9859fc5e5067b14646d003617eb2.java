package Java_2.j2_lesson6;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class Client {
    public static final int PORT = Server.SERVER_PORT;
    public static final String HOST = "localhost";
    private static final String CLIENT_NAME = "Client";


    public static void main(String[] args) {
        Thread inputThread = null;

        try (Socket clientSocket = new Socket(HOST, PORT)) {
            DataInputStream fromServer = new DataInputStream(clientSocket.getInputStream());
            DataOutputStream toServer = new DataOutputStream(clientSocket.getOutputStream());

            inputThread = Interactions.processInput(fromServer);
            Interactions.processOutput(CLIENT_NAME, toServer);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (inputThread != null) inputThread.interrupt();
        }
    }

}