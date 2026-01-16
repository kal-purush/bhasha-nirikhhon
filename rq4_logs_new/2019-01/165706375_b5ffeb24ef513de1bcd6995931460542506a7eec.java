package com.streamsets.tcpstream;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Date;

public class TcpStream {

  public static void main(String[] args) {

    final String hostname = "localhost";
    final int port = 55_555;
    final int RECORD_COUNT = 100_000;
    while (true) {
      final long startTime = System.currentTimeMillis();
      while (true) {
        long batchTime = System.currentTimeMillis();
        try (Socket clientSocket = new Socket(hostname, port)) {
          DataOutputStream output = new DataOutputStream(clientSocket.getOutputStream());

          for (int i = 0; i < RECORD_COUNT; i++) {
            String cheese = "hello there. record " + i + "!\n";
            output.writeBytes(cheese);
          }
        } catch (IOException e) {
          System.err.println("exception " + e.getMessage());
          e.printStackTrace();
          break;
        }
        long curr = System.currentTimeMillis() - batchTime;

        try {
          Thread.sleep(10);
        } catch (InterruptedException ex) {
          System.out.println("Exception: " + ex.getMessage());
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            System.out.println("Interruped Exception: " + e.getMessage());
          }
        }
        long elapsed = (System.currentTimeMillis() - startTime)/1000;
        System.out.print(String.format("%s  %d:%02d:%02d  batch elapsed %d\n",
            new Date(System.currentTimeMillis()),
            elapsed /3600,
            elapsed % 3600 / 60,
            elapsed % 60,
            curr));
      }
    }
  }
}