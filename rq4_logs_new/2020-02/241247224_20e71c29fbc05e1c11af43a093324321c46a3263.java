/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package desencryptedchat;

import java.net.*;
import java.util.*;
import java.io.*;

/*
 *
 * @author woah dude
 */
public class DESencryptedChat {
    public static Scanner scan = new Scanner(System.in);
    private static String privateKey = "";
    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception{
        System.out.println("Enter your DES key here: ");
        String semiKey = scan.nextLine();
        privateKey = ChatHelper.keyConverter(semiKey);
        
        System.out.println("Enter your HMAC key here: ");
        String hmacKey = scan.nextLine();

        Socket sock = null;
        String input;
        int port = 5000;
        
        
        while(sock == null){ // host or join menu. stuck here until a valid socket is produced.
            System.out.println("\n\nHost or join?\n\n1. Host\n2. Join");
            input = scan.nextLine();
            
            
            switch (input) {
                case "1":
                case "1.":
                    sock = hostMethod(port);
                    break;
                case "2":
                case "2.":
                    sock = joinMethod(port);
                    break;
                default:
                    System.out.println("\nInvalid, please try again.");
                    break;
            }
        }
        
        
        senderThread(sock);
        
        
        System.out.println("\nClosing chat program...");
        
        scan.close();
    }
    
    public static Socket hostMethod(int inPort) throws IOException{
        Socket ret = null;
        
     
        
        try{
            ServerSocket serverSocket = new ServerSocket(inPort);
            System.out.println("\n\nAwaiting connection...");
            ret = serverSocket.accept();
            
            System.out.println("\n" + ret.getInetAddress().toString() + " connected!");
        }
        catch(IOException e){
            System.out.println("\n" + e);
            throw e;
        
        }
        return ret;
    }
    
    public static Socket joinMethod(int inPort) throws IOException{
        Socket ret = null;
        String ip;
        
        try{
            System.out.println("\n\nPlease enter ip: ");
            ip = scan.nextLine();
            
            ret = new Socket(ip, inPort);
        }
        catch(IOException e){
            System.out.println("\n" + e);
            throw e;
        }
        
        return ret;
        
        
        
        
       
        
       
    }
    
    public static void senderThread(Socket sock) throws Exception{
        // fork here
        listenerThread lThread = new listenerThread(sock);
        lThread.start(); // starts listening thread
        
        boolean stopFlag = false;
        // key insertion
//        System.out.println("Enter your private key. Your partner needs the key.");
//        key = scan.nextLine();
//        KeyGenerator.setKey(key);
        
        PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
        
        System.out.println("\nLocal sender ready.");
        while(!stopFlag){
            try{
                String userInput = scan.nextLine();
                if(userInput.toLowerCase().equals("stop")){
                    stopFlag = true;
                    
                    
                }
                
                System.out.println("Me: " + userInput);
                
                // ENCRYPTION GOES HERE. SEND OUT ENCRYPTED CIPHERTEXT INSTEAD
                // OF userInput
                
                // insert while loop for longer than 64 bits here
                
                EncryptDecrypt ed = new EncryptDecrypt(userInput);
                //
//                String key
//                        = "00010011"
//                        + "00110100"
//                        + "01010111"
//                        + "01111001"
//                        + "10011011"
//                        + "10111100"
//                        + "11011111"
//                        + "11110001";
                
                KeyGenerator kg = new KeyGenerator(privateKey);
                
                String[] RoundKeyArray = kg.keyGenerator(privateKey);
                                
                String ct = ed.Encrypt(ed.getInitialMessage(), RoundKeyArray);
                
                System.out.println("\tKey: " + privateKey);
                System.out.println("\tCypherText in binary: " + ct);
                System.out.println("\tCypherText translated from binary: " + ChatHelper.binaryStringToText(ct));
                
                // end while loop
                
                out.println(ct);
            }   
            
                
            catch(Exception e){
                System.out.println("\n" + e);
                lThread.end();
                sock.close();
                throw e;
            }
        }
        
        lThread.end();
        sock.close();
        
        System.out.println("\nSender-aspect done running.");
        
    }
    
    public static String getKey(){
        return privateKey;
    }
    
    
}