package JAVA_LEVEL_2.Java_lvl2_homework.git.lesson7.lection7.client.src.main.java;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class ClientNetwork {
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;

    public void connect() {
        try {
            socket = new Socket("localhost", 8189);
            in = new DataInputStream(socket.getInputStream());
            out = new DataOutputStream(socket.getOutputStream());
            new Thread(() -> {
                boolean goOn = true;
                try {
                    while(goOn) {
                        String msg = in.readUTF();
                        if(msg.equalsIgnoreCase("/end")){
                            goOn = false;
                        }
                        else if(msg.startsWith("/clients ")) {
                            // rassilka imeyushihsya klientov
                        }
                        else {
                            // vivod soobsheniya
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean sendMessage(String msg) {
        try {
            out.writeUTF(msg);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public void closeConnection () {
        try {
            in.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}