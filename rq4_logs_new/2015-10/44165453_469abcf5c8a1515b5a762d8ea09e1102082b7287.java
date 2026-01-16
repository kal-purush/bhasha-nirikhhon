/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Client;

import java.net.Socket;

/**
 *
 * @author sonng_000
 */
public class ReceiveFile {
    
    private Socket socket;
    private String filepath; // filepath: duong dan toi file can gui. VD: C:\Users\sonng_000\Documents\abc.txt
    
    public ReceiveFile(Socket socket, String filepath) {
        this.socket = socket;
        this.filepath = filepath;
    }
    
    public void receiveFile() { 
        // hien thuc gui file qua socket, gia su socket da duoc ket noi
        
    }
}