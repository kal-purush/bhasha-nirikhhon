import java.io.*;
import java.net.*;

public class MyClient {
    public static void main(String[] args){
        Socket clientSocket = null;
        BufferedReader in = null;
        int ip;
        try {//Keep this number. For the pace server this is the number we must use.
            ip = 4040; //("vulcan.seidenberg.pace.edu");
            //InetAddress host = InetAddress.getByName( "54.211.11.43");
            InetAddress host = InetAddress.getByName( "localhost");
            clientSocket = new Socket(host, ip);
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            String s = in.readLine();
            while(s == null)	//As long as there is nothing in the stream keep reading
                s = in.readLine();
            System.out.println(s);
            in.close();
            clientSocket.close();
        }catch (IOException e) {
            System.out.println("Error: " + e);
            System.exit(0);
        }
    }
}