package edu.nps.muster.gateway;

// This is the main class that manages the threads which capture each muster request

public class threadingExample extends Thread implements mqttCaller {

    // Currently this is just a sample threading code which prints "Hello World"
    // using the default constructor

    private String msg;
    private mqttConnection mqttConn = new mqttConnection(this);

    private threadingExample(String message ) {
        msg = message;
        mqttConn.subscribe("Test");
    }

    public void run() {
        print(msg);
        mqttConn.publish("Test", msg);

        //try { sleep(10000); } catch (Exception e) { e.printStackTrace(); }

        //mqttConn.disconnect();
    }


    @Override
    public void messageReceived(String topic, String message) {
        print("-------------------------------------------------");
        print("| Topic:" + topic);
        print("| Message: " + message);
        print("-------------------------------------------------");

    }

    private void print(Object out) { System.out.println(out.toString()); }

    public static void main(String args[]) {
        (new threadingExample("Hello World")).start();
    }
}
