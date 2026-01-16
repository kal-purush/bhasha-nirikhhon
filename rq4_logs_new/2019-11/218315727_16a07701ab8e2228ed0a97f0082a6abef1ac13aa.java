package main;

import multicasting.MulticastGroup;
import rmi.RMIServer;

public class Main {
  public static void main(String[] args) {
    Integer multicastPort = Integer.parseInt(args[0]);
    String multicastAddress = args[1];
    Integer serverPort = Integer.parseInt(args[2]);

    new Thread() {
      public void run() {
        new MulticastGroup(multicastAddress, multicastPort).listenMulticastGroup();
      }
    }.start();

    new Thread() {
      public void run() {
        new RMIServer(serverPort).startPlaceManagerServer();
      }
    }.start();
  }
}