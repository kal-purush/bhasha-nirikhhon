/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package NMEAProxy;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 *
 * @author Bernhard Bagyura
 */
public class ClientConnection implements Runnable {
    
    private final ServerSocket myServerSocket;
    private final Starter starter;
    private final int akt;
    private Socket mysocket;
    private OutputStream myoutputstream;
    private boolean isRestart;
    
    public ClientConnection(ServerSocket myServerSocket, int akt, Starter starter, boolean isRestart) {
        this.myServerSocket = myServerSocket;
        this.akt = akt;
        this.starter = starter;
        this.isRestart = isRestart;
    }
    
    @Override
    public synchronized void run() {
        try {
            mysocket = myServerSocket.accept();
            System.out.println("Verbindung akzeptiert: " + this.toString());
            myoutputstream = mysocket.getOutputStream();
            
            starter.incrementAktive();
        
        // Startet eine neu Instanz von Client Connection
        // wenn sich ein Client verbunden hat und es kein Restart ist
            if (isRestart == false) {
                starter.nextclient(akt + 1);
            }
        } catch (Exception e) {
            System.out.println(e);
        }
        try {
            System.out.println("Thread waiting: " + akt);
            wait(); 
            
        } catch (Exception e) {
        }
        System.out.println("Thread finished: " + akt);
    }
 
    public void write (byte a){
        
        //myoutputstream.write(a);
        
        try {
            myoutputstream.write(a);
        } catch (IOException e) {
            System.out.println("Akt: "+ akt + "ClientConnection Exception in Write: " + e);
            stopme();
            starter.decrementAktive();
            starter.restartConnection(akt);
        } catch (Exception e) {
            //System.out.println("Was auch immer");
        }
    }
    
    public synchronized void stopme(){
        try {
             mysocket.close();
             myoutputstream.close();
             System.out.println("Class ClientConnection Socket close for Thread: " + this.toString());
             notify();
             
        } catch (Exception e) {
            System.out.println("Class ClientConnection exception Close Socket Connection: " +  e);
        }
    }
    
}