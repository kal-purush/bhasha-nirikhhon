/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package networklib.Server;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;



/**
 *
 * @author Thomas
 */
public class ServerTCPv2 extends Thread {
    private final ServerSocket ssock;
    private final ServerConsole Log;
    private final TasksSource tasks;
    private final ServerBuisness MainProgram;
    
    private final int ThreadsCount;
    private final LinkedList<ThreadSocketService> threads;
    
    
    public ServerTCPv2(int port, TasksSource s, ServerConsole l, int countthreads, ServerBuisness MainProg) throws IOException{
        ssock = new ServerSocket();
        ssock.bind(new InetSocketAddress(InetAddress.getByName("0.0.0.0"),port));
        
        tasks = s; 
        Log = l;
        ThreadsCount = countthreads;
        MainProgram = MainProg;
        threads = new LinkedList<>();
    }
    
    
    public void Shutdown() throws IOException{
        //A l'avenir il faudra que le serveur envoi un message de fermeture au client
        for(ThreadSocketService th : threads){
            th.interrupt();
            th.Shutdown();
        }
        Log.Trace("serveur#fermeture des threads service#main");
        
        ssock.close();
        Log.Trace("serveur#fermeture listen socket#main");
        
        Log.Trace("serveur#fully shutdown#main");
        System.out.println("************ Fin exécution Serveur");
    }

    @Override
    public void run() {
        //Create Thread to Accept new Connexion
        for(int i = 0 ; i < ThreadsCount ; i++){
            ThreadSocketService th = new ThreadSocketService(tasks, "Thread n°" + String.valueOf(i));
            th.setName("Serveur-TCP Thread Service n°" + String.valueOf(i));
            th.start();
            
            threads.add(th);
        }
        Log.Trace("serveur#pool thread créer#main");
        ServerTCPv2.listDebugThreads();
        
        Socket serviceSock;
        int j = 0;
        //le Is Interrupted() permet de quitter la boucle du thread en cas d'interruption.
        while(!isInterrupted()){
            j++;
            
            try {
                System.out.println("************ Serveur en attente");
                if(j == 1) Log.Trace("serveur#attente accept#main");
                
                serviceSock = ssock.accept(); 
                if(j == 1) Log.Trace(serviceSock.getRemoteSocketAddress().toString()+"#accept success#thread serveur");
                
            } catch (IOException ex) {
                Logger.getLogger(ServerTCPv2.class.getName()).log(Level.SEVERE, null, ex);
                continue;
            }
            
            
            //Création d'un Runnable sur base de la requete de nouvelle connexion récupéré a l'instant.
            Runnable todo = MainProgram.createRunnable(serviceSock, Log);
            if (todo != null)
            {
                    //On ajoute la nouvelle Connexion dans la liste des requètes a traitée
                    tasks.recordTask(todo);
                    if(j == 1) Log.Trace(serviceSock.getRemoteSocketAddress().toString()+"#Tentative Nouvelle Connexion Réussie#thread serveur");   
            }
            else{
                //On ne fait rien
                if(j == 1) Log.Trace(serviceSock.getRemoteSocketAddress().toString()+"#Tentative Nouvelle Connexion Annulée#thread serveur");
            } 
        }
    }
    
    
    
    
    public static void listDebugThreads(){
        Set<Thread> threads = Thread.getAllStackTraces().keySet();
 
        for (Thread t : threads) {
            String name = t.getName();
            Thread.State state = t.getState();
            int priority = t.getPriority();
            String type = t.isDaemon() ? "Daemon" : "Normal";
            
            if(name.contains("Serveur-TCP")){
                System.out.printf("%-20s \t %s \t %d \t %s\n", name, state, priority, type);
            }
        }
    }

}