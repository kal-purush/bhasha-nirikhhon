package javarmi_server;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import imagermi.ImageInterface;
import java.awt.Color;
import java.rmi.*;
import java.rmi.server.*;

/**
 *
 * @author AdminMobility
 */
public class Image extends UnicastRemoteObject implements ImageInterface {

    public Image()throws RemoteException {
        super();
    }
    @Override
    public int[] toBW(int startX, int startY, int w, int h, int[] rgbArray, int offset, int scansize) throws RemoteException {
        
        int[] output = new int[h*w];
        
        int x, y;
        for (y=startY; y<h; y++) {
            for (x=startX; x<w; x++) {
                Color c = new Color(rgbArray[x + scansize*y]);
                output[x + scansize*y] = (int)(c.getRed() * 0.299 + c.getGreen() * 0.587 + c.getBlue() * 0.114);
            }
        }
        
        return output;
    }
    
}