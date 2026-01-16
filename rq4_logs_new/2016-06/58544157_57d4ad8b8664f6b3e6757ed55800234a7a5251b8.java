package Client;


import Ui.IhmGraph;
import java.io.IOException;

import java.net.*;
import java.util.logging.Level;
import java.util.logging.Logger;



public class Clients {
    Socket socket;
    int port;
    String adresse;
    String nomJoueur;
    IhmGraph ihmGraph;
    
    public Clients(int port, String adresse, String nomJoueur, IhmGraph ihmGraph){
        
        this.port=port;
        this.adresse=adresse;
        this.nomJoueur=nomJoueur;
        if ("".equals(adresse)){
            adresse="localhost";
        }
        if (port==0){
            port=11111;
        }
        try {
            socket = new Socket(adresse, port);
        } catch (IOException e) {
            System.err.println("L'adresse ou le port est incorrect");
        }
        this.ihmGraph=ihmGraph;
        
        
        
        
        
        
        
        
        
        
        
        try {
            socket.close();
        } catch (IOException ex) {
            Logger.getLogger(Clients.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }

}