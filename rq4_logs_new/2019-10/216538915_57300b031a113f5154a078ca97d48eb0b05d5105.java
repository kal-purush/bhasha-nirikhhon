import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

public class Broker {
    /*
    public static void main(String[] args) {
        System.out.println("Attempting connection...");
        try {
            //attempt connection to server localhost via port 5000
            Socket socket = new Socket("localhost", 5000);

            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            System.out.println("Received string: [");
            while (!in.ready()) {}
            System.out.println(in.readLine()); //read on line and output it
            System.out.println("]\n");

            //end broker
            in.close();
        }
        catch (Exception e) {
            System.out.println("Whoops! Broker failed!\n");
        }
    }

     */
    public static void main(String[] args) throws IOException {

//        if (args.length != 2) {
//            System.err.println(
//                    "Usage: java EchoClient <host name> <port number>");
//            System.exit(1);
//        }

//        String hostName = args[0];
//        int portNumber = Integer.parseInt(args[1]);
            String hostName = "localhost";
        int portNumber = 8000;

        try (
                Socket kkSocket = new Socket(hostName, portNumber); //open a socket that is connected to the server running on the specific port number
                PrintWriter out = new PrintWriter(kkSocket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(kkSocket.getInputStream()));
        ) {
            BufferedReader stdIn =
                    new BufferedReader(new InputStreamReader(System.in));
            String fromServer;
            String fromUser;

            //implement communication between client and server
            while ((fromServer = in.readLine()) != null) {
                System.out.println("Server: " + fromServer); //server speaks first, so client must listen
                if (fromServer.equals("exit")) //if server says this, end
                    break;

                fromUser = stdIn.readLine(); //take input
                if (fromUser != null) {
                    System.out.println("Client: " + fromUser);
                    out.println(fromUser);
                }
            }
        } catch (UnknownHostException e) {
            System.err.println("Don't know about host " + hostName);
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to " +
                    hostName);
            System.exit(1);
        }
    }
}