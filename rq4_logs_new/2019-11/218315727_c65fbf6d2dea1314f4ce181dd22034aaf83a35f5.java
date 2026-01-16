package main;

import java.io.IOException;
import java.net.InetAddress;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import places.PlaceManager;

public class RMIServer {
  public static void main(String[] args) {
    Integer multicastPort = Integer.parseInt(args[0]);
    String multicastAddress = args[1];
    Integer serverPort = Integer.parseInt(args[2]);

    PlaceManager placeList = startRMIServer(serverPort);

    
    new Thread() {
      public void run() {
        while (true) {
          // Sends own IP in Multicast
          try {
            placeList.sendMessage(multicastAddress, multicastPort,
                "rmi://" + InetAddress.getLocalHost().getHostAddress() + ":" + serverPort + "/placelist");
          } catch (IOException e) {
            System.out.println(e.getMessage());
          }
          
          // Holds for X milliseconds
          try {
            Thread.sleep(10000);
          } catch (InterruptedException e) {
            System.out.println(e.getMessage());
          }
        }
      }
    }.start();
   
    new Thread() {
      public void run() {
        // Listens to other IPs in Multicast
        try {
          placeList.listenNewHosts(multicastAddress, multicastPort);
        } catch (IOException e) {
          System.out.println(e.getMessage());
        }
      }
    }.start();
  }

  private static PlaceManager startRMIServer(Integer serverPort) {
    Registry r = null;
    PlaceManager placeList = null;

    try {
      System.out.println("Creating registry on port: " + serverPort);
      r = LocateRegistry.createRegistry(serverPort);
    } catch (RemoteException a) {
      System.out.println(serverPort + " is already used.");
      try {
        System.out.println("Getting registry on port: " + serverPort);
        r = LocateRegistry.getRegistry(serverPort);
      } catch (NumberFormatException | RemoteException e) {
        System.out.println("Error getting registry of port: " + serverPort);
      }
    }

    try {
      placeList = new PlaceManager(serverPort);
      r.rebind("placelist", placeList);

      System.out.println("PlaceManager running on port: " + serverPort);
    } catch (Exception e) {
      System.out.println("Error trying to run PlaceManager on port: " + serverPort);
    }

    return placeList;
  }
}