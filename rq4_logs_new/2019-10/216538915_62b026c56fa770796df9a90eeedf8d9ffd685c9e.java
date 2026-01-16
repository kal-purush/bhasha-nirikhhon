package Router;

import java.lang.*;
import java.io.*;
import java.net.*;

class Client{
    public static void main(String args[]){
        try {
            Socket sock = new Socket("localhost", 5000);
            BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
            System.out.print("Received string: ");
            while (!in.ready()){}
            System.out.println(in.readLine());
            
            System.out.print("\n");
            in.close();
        } 
        catch (Exception e) {
            System.out.print("NOPE");
        }
    }
}