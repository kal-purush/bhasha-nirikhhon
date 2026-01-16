package Protocol;

import java.net.Socket;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Merijn
 */
public class ServerMessageReceiver 
{
       
    public ServerMessageReceiver()
    {
        
    }
    
    public Message processMessage(Message message)
    {
        //CHECK WHAT KIND OF MESSAGE
        //IF DATABASE ACTIVITY OR SAVING 
        //RETURNS MESSAGE
        return null;
    } 
    
    /**
     * THIS METHOD PROCESSES MESSAGES WITH THE REGISTER TYPE
     * IF PERSONAL TYPE == 0 WE KNOW ITS A CIVILIAN
     * @param message 
     */
    private void processRegister(Message message)
    {
        
    }
}