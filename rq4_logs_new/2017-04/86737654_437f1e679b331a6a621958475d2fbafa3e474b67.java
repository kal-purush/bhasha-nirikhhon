/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.upo.connect4;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Marco
 */
public class Cell {
    private String state;
    
    private String empty = "icons/empty.png";
    private String red = "icons/red.png";
    private String yellow = "icons/yellow.png";

    public Cell() {
       
            state=empty;
            
    }

    public String getState() {
        return state;
    }

     
    public boolean setRed(){
        boolean inserted = false;
        if(state.equals(empty)){
            state = red;
            inserted = true;
        }
        return inserted;
    }
    public boolean setYellow(){
       boolean inserted = false;
        if(state.equals(empty)){
            state = yellow;
            inserted = true;
        }
        return inserted;
    }
}