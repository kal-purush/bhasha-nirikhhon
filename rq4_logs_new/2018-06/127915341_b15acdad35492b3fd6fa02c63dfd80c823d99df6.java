/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package scs_asteroide;

import iut.Jeu;
import iut.Objet;

/**
 *
 * @author aurian
 */
public class Vie extends iut.Objet {
    
    private int vieRestante = 3;
    
    public Vie(Jeu g, String nom, int x, int y){
        super(g, nom, x, y);
    }
    
    public void enleverVie(){
        if(vieRestante > 0){
            vieRestante --;
        }
        if(vieRestante == 0){
            //coder fin du jeu car le joueur a perdu
        }
    }


    @Override
    public String getTypeObjet() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void evoluer(long l) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean testerCollision(Objet objet) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void effetCollision(Objet objet) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}