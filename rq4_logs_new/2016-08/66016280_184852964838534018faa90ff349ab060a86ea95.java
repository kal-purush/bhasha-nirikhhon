package edu.nps.muster.gateway;

import java.util.*;
import java.text.*;

// This is the main class that manages each captured muster request

public class gatewayMain implements mqttCaller {

    private musterDatabase musterDB;
    private mqttConnection mqttConn;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  //2016-08-18 18:48:05.123456

    // Constructor which connects to MQTT, subscribes, and connects to mySQL database.
    private gatewayMain() {
        try {
            musterDB = new musterDatabase();
            mqttConn = new mqttConnection(this);
            mqttConn.subscribe("Node1");
        } catch( Exception e) { e.printStackTrace(); }
    }

    // Do something when we receive message from MQTT (which calls this method directly)
    public void messageReceived(String topic, String message) {
        String date = message.split(",")[1];
        String nodeID = topic;
        String MAC = message.split(",")[0];
        print("-------------------------------------------------");
        try {
            print("| Date: " + date);
            print("| NodeID: " + nodeID);
            print("| StudentMAC: " + MAC);
            musterDB.addMuster(date, MAC, nodeID);
            print("| Email: " + musterDB.deviceToStudent(MAC));
        } catch(Exception e) { print("| Error: "+e.getMessage()); }
        print("-------------------------------------------------");

    }

    // Helper method to make code prettier
    private void print(Object out) { System.out.println(out.toString()); }

    // Tester method to verify mustering works with mpakin1@nps.edu's 1:2:3:4 mac address
    private void testMuster(){

        print("-------------------------------------------------");
        try {
            print("| mustered today: " + musterDB.didMusterToday("mpakin1@nps.edu"));
        } catch (Exception e) { print("| Error: "+e.getMessage()); }
        print("-------------------------------------------------");
        try { Thread.sleep(1000); } catch(Exception e) {e.printStackTrace();}

        // Send test muster.
        String nodeID = "Node1";
        String MAC = "1:2:3:4";
        String date = sdf.format(new Date());
        mqttConn.publish(nodeID, MAC + "," + date);

        try { Thread.sleep(1000); } catch(Exception e) {e.printStackTrace();}
        print("-------------------------------------------------");
        try {
            print("| mustered today: " + musterDB.didMusterToday("mpakin1@nps.edu"));
        } catch (Exception e) { print("| Error: "+e.getMessage()); }
        print("-------------------------------------------------");

    }

    public static void main(String args[]) {
        //new gatewayMain();
        new gatewayMain().testMuster();
    }
}
