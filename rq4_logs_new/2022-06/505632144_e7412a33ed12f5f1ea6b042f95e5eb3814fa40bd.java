/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.laberintorecursivo;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author luisa
 */
public class Laberinto {

    private char L[][];

    public Laberinto(char[][] l) {
        L = l;
    }

    public char[][] getL() {
        return L;
    }

    public void setL(char[][] l) {
        L = l;
    }

    public boolean resolucionLaberinto(int x, int y){
        if(L[x][y]=='F')
            return true;
        if(L[x][y]=='#'|| L[x][y]=='.')
            return false;
        L[x][y]='.';
        imprimirLaberinto();
        if(resolucionLaberinto(x, y+1)||resolucionLaberinto(x-1, y)||resolucionLaberinto(x, y-1)||resolucionLaberinto(x+1, y)){ // (Arriba o Izquierda o Abajo o Derecha )
            return true;
        }
        imprimirLaberinto();
        L[x][y] = ' ';
        return false;
    }

    public void imprimirLaberinto(){
        try 
        {
            Thread.sleep(300);
            Runtime.getRuntime().exec("cls");
        }
        catch (IOException ex) {
            Logger.getLogger(Laberinto.class.getName()).log(Level.SEVERE, null, ex);
        
        } 
        catch(InterruptedException e){
            e.getMessage();
        }
        for(int i=0;i<L.length;i++){
            for(int j=0;j<L[0].length;j++){
                System.out.print(L[i][j]+" ");
            }
            System.out.println("");
        }
    }
}