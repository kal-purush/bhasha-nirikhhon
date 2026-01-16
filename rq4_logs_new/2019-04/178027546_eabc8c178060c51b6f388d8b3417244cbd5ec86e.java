package com.example.mi_b_wizard.Network;



import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

public class Client extends Thread {
        Socket socket;
        String host;
        SendReceive sendReceive;

        public Client(InetAddress hostAddress) {
            host = hostAddress.getHostAddress();
            socket = new Socket();
        }

        @Override
        public void run() {
            super.run();

            try {
                socket.connect(new InetSocketAddress(host, 8888), 1000);
                sendReceive = new SendReceive(socket);
                sendReceive.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
