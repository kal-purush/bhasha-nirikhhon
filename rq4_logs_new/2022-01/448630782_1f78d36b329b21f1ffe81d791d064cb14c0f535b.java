package net.htlgkr.groupK.chess;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Scanner;

public class Client {
    private InetSocketAddress address;

    public Client(String hostname, int port) {
        address = new InetSocketAddress(hostname, port);
    }

    public static void main(String[] args) {
        System.out.println("[Client] enter ip address");
        Scanner s = new Scanner(System.in);
        String ipAddress = s.nextLine();
        System.out.println("[Client] enter port");
        int port = Integer.parseInt(s.nextLine());
        Client client = new Client(ipAddress, port);
        client.startClient();
    }

    public void startClient() {
        try {
            Socket socket = new Socket();
            socket.connect(address);
            System.out.println("[Client] client connected");

            System.out.println("[Client] enter message");
            Scanner s = new Scanner(System.in);
            String msg = s.nextLine();

            PrintWriter pw = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
            pw.println(msg);
            pw.flush();

            pw.close();
            System.out.println("[Client] client closed");
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}