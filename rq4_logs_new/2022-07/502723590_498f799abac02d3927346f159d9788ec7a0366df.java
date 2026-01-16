package Socet;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Scanner;

public class ClientHost1 {
    public static void main(String[] args) {
        int serverPort = 2000;
        String adress = "127.0.0.1";
        try {
            InetAddress ipAdress = InetAddress.getByName(adress);
            try (Socket socket = new Socket(ipAdress, serverPort)) {
                System.out.println("server finder!");
                DataInputStream in = new DataInputStream(socket.getInputStream());
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());

                Scanner scanner = new Scanner(System.in);
                String line;
                while (socket.isConnected()) {
                    line = scanner.nextLine();
                    out.writeUTF(line);
                    out.flush();
                    line = in.readUTF();
                    System.out.println(line);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}