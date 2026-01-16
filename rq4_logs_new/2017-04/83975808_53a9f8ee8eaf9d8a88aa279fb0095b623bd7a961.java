/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gameoflife;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Espen
 */
public class FileIO {
 
    protected void writeBoardToFile(GameBoardModel gameBoardModel) throws IOException{
        File outputFile = new File ("savegame/testfile1.dat");
        DataOutputStream os;
    
        os = new DataOutputStream(new FileOutputStream(outputFile));
        int xmax = gameBoardModel.getXmax();
        int ymax = gameBoardModel.getYmax();
        
        os.writeInt(xmax);
        os.writeInt(ymax);
        
        int counter = 0;
        boolean last=false;
        boolean atStart=true;
        for (int i=0; i<xmax; i++){
            for (int j=0; j<ymax; j++){
                boolean current = gameBoardModel.getCellIsAlive(i, j);
                if (atStart){
                    last=current;
                    atStart=false;
                }
                
                if (current == last && counter < 127){
                    counter ++;                    
                }
                else{                                                          
                    writeCompressedBlock(os, counter, last); 
                    counter = 1;
                    last=current;
                    System.out.println("tekst"+i+j);
                }
            }
        }
        writeCompressedBlock(os, counter, last);
        
        os.close();
    }
    
    protected void readFile() throws IOException{
        File inputFile = new File ("savegame/testfile1.dat");
        DataInputStream os;
    
        os = new DataInputStream(new FileInputStream(inputFile));
        int xmax = os.readInt();
        int ymax = os.readInt();
        System.out.println(xmax);
        System.out.println(ymax);
        while(true){
            try{
                byte output = os.readByte();
                System.out.println(output);
            }
            catch (EOFException e){
                break;
            }
        }
        os.close();
    }
    
        protected void readBoardFromFile(GameBoardModel gameBoardModel) throws IOException{
        File inputFile = new File ("savegame/testfile1.dat");
        DataInputStream os;
    
        os = new DataInputStream(new FileInputStream(inputFile));
        int xmax = os.readInt();
        int ymax = os.readInt();
        gameBoardModel.setXmax(xmax);
        gameBoardModel.setYmax(ymax);
        int startX = 0;
        int startY = 0;
        boolean [][] cellIsAliveArray = new boolean[xmax][ymax];
        while(true){
            try{
                byte output = os.readByte();
                byte fillNumber = (byte) Math.abs(output);
                boolean isAlive = false;                
                if (output>0){
                    isAlive = true;
                }                
                    int[] startValues = fillArray(cellIsAliveArray,startX, startY, fillNumber, isAlive, xmax, ymax);
                    startX = startValues[0];
                    startY = startValues[1];
            }
            catch (EOFException e){
                break;
            }
        }
        os.close();
    }
        
    private int[] fillArray(boolean [][] cellIsAliveArray, int startX, int startY, byte fillNumber, boolean isAlive, int xmax, int ymax){
        int nextX = startX;
        int nextY = startY;
        int[] startValue;
        startValue = new int[2];
        for (int i = 0; i < fillNumber; i++){
            cellIsAliveArray[nextX][nextY] = isAlive;
            if(nextY+1>ymax){
                nextX+=1;
                nextY=0;
            }
            else{
                nextY+=1;
            }
            startValue[0] = nextX;
            startValue[1] = nextY;            
        }
        return startValue;
    }
    
    private void writeCompressedBlock(DataOutputStream os, int amount, boolean alive) throws IOException{        
        byte codedByte = (byte) amount;
        if(!alive){
            codedByte = (byte) (-1*codedByte);
        }
        os.writeByte(codedByte);
    }
    
}