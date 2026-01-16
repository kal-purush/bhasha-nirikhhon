import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;

public class broker {
    public static void main(String[] args) {
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
}