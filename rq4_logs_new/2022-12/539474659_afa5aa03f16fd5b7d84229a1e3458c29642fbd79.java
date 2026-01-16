/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ProtocolFUCAMP;

import ActivitiesDataLayer.db;
import ActivitiesDataLayer.entities.Activities;
import ActivitiesDataLayer.entities.Voyageurs;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
import networklib.Server.Request;
import networklib.Server.ServerConsole;

/**
 *
 * @author Thomas
 */
public class UnlistRequest implements Request, Serializable{
    
    Activities activities;
    Voyageurs voyageurs;
    
    public UnlistRequest(Activities act, Voyageurs v){
        activities = act;
        voyageurs = v;
    }
    
    @Override
    public Runnable createRunnable(Socket s, ServerConsole cs) {
        return () -> {
            System.out.println( Thread.currentThread().getName() + ": Execution d'une Tache pour " +  s.getRemoteSocketAddress().toString());
            try {
                s.getOutputStream().flush();
                TASK(s,cs);
            } catch (IOException ex) {
                Logger.getLogger(UnlistRequest.class.getName()).log(Level.SEVERE, null, ex);
            }
        };
    }
    
    private void TASK(Socket sock, ServerConsole log) throws IOException{
        ObjectOutputStream oos = new ObjectOutputStream( sock.getOutputStream());
        if(activities == null){
            log.Trace(sock.getRemoteSocketAddress().toString() + "# Register error: unkown activities#" + Thread.currentThread().getName());
            oos.writeObject(new UnlistResponse(UnlistResponse.UNKOWNACTIVITIES, "Activitiée inconnue"));
            return;
        }
        
        if(voyageurs == null){
            log.Trace(sock.getRemoteSocketAddress().toString() + "# Register error: unkown Client#" + Thread.currentThread().getName());
            oos.writeObject(new UnlistResponse(UnlistResponse.UNKNOWNCLIENT, "Client inconnu"));
            return;
        }
        
        
        try {
            db.UnlistToActivities(activities, voyageurs);
            oos.writeObject(new UnlistResponse(UnlistResponse.SUCCESS, "Client Désinscrit"));
            log.Trace(sock.getRemoteSocketAddress().toString() + "# UnlistRequest: " + voyageurs.getNomVoyageur() + ", " 
                    + voyageurs.getPrenomVoyageur() + " #" + Thread.currentThread().getName());
            
        } catch (SQLException ex) {
            log.Trace(sock.getRemoteSocketAddress().toString() + "# Register SQL Error: " + ex.getErrorCode() + " #" + Thread.currentThread().getName());
            oos.writeObject(new UnlistResponse(UnlistResponse.BADDB, ex.getMessage()));
        } catch(Exception ex){
            log.Trace(sock.getRemoteSocketAddress().toString() + "# Register Unkown Error: " + ex.getMessage() + " #" + Thread.currentThread().getName());
            oos.writeObject(new UnlistResponse(UnlistResponse.UNKOWN, ex.getMessage()));
        }
    }
}