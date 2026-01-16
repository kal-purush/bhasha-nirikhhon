/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import ProtocolROMP.*;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;
import networklib.Server.ServerConsole;

/**
 *
 * @author Thomas
 */
public class MainServBuisness extends Request {
    private boolean isLogged;
    
    public MainServBuisness() {
        this.isLogged = false;
    }
    
    @Override
    public void Task(Socket sock, ServerConsole log) throws IOException, ClassNotFoundException {
        ObjectInputStream ios = new ObjectInputStream(sock.getInputStream());
        
        while(true){
            Object r = ios.readObject();
            if(!(r instanceof ProtocolROMP.Request)){
                isLogged = false;
                return;
            }
            Request req = (Request) r;
            
            if(req instanceof ProtocolROMP.TimeOut){
                isLogged = false;
                return;
            }
            
            if(!isLogged){
                if(req instanceof LoginRequest){
                    LoginRequest lr = (LoginRequest) req;
                    //Execute Login Task
                    lr.Task(sock, log);
                    isLogged = lr.Logged;
                }
            }
            else{
                req.Task(sock, log);
            }
        }
    }
}