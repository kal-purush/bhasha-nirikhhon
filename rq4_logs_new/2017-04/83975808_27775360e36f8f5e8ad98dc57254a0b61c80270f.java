/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gameoflife;

/**
 *
 * @author Espen
 */
public class GameBoardCell {
    private int x;
    private int y;
    boolean[][] cellIsAliveArray;
    
    public GameBoardCell(int x, int y, boolean[][] cellIsAliveArray){
        this.x=x;
        this.y=y;
        this.cellIsAliveArray = cellIsAliveArray;
        
    }

    public int getX(){
        return x;
    }
    
    public int getY(){
        return y;
    }
    
    public boolean isAlive(){
        return cellIsAliveArray[x][y];
    }
    
    
}