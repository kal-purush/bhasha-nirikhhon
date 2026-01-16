package com.munteanu.sockets;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultiClientServer {

  private static final int NUMBER_OF_THREADS = 5;

  private ServerSocket server;
  private ExecutorService executor;

  public void start(int port) {
    try {
      server = new ServerSocket(port);
      executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
      while (true) {
        Socket client = server.accept();
        Runnable worker = new RequestHandler(client);
        executor.execute(worker);
      }
    } catch (IOException e) {
      System.out.println(e.getMessage());
    } finally {
      if (executor != null) {
        executor.shutdownNow();
      }
      stop();
    }
  }

  public void stop() {
    try {
      server.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    System.out.println("Starting main thread");
    if (args.length < 1) {
      System.err.println("Usage: java MultiClientServer <port_number>");
      System.exit(1);
    }

    int portNumber = Integer.parseInt(args[0]);

    MultiClientServer multiClientServer = new MultiClientServer();
    multiClientServer.start(portNumber);
  }
}