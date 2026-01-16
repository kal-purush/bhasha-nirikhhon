package JAVA_LEVEL_2.Java_lvl2_homework.git.lesson6.lection6;
import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

public class Client {
    public static void main(String[] args) {
        startTextClient();
        //startObjectClient();
    }

    private static void startTextClient() {
        try (Socket socket  = new Socket("localhost", 8180);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream());
             Scanner sc = new Scanner(System.in)){
            String myMessage = "";
           // String serverMessage = "";
            Thread serverReader = new Thread(() -> {
                String serverMessage = "";
                try {
                    while(!socket.isClosed()) {
                        serverMessage = in.readLine();
                        System.out.println(serverMessage);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            serverReader.start();
            do{
                //serverMessage = in.readLine();
                //System.out.println(serverMessage);
                myMessage = sc.nextLine();
                out.println(myMessage);
                out.flush();
            }while(!myMessage.equalsIgnoreCase("stop"));

        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void startObjectClient() {
        try (Socket socket  = new Socket("localhost", 8180);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())){
            //Cat vasilyi = new Cat(3,"Vasilyi");
            //out.writeObject(vasilyi);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}