/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sudoku;

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.JButton;

/**
 *
 * @author Clément
 */

public class Bouton extends JButton implements ActionListener {
    
    Case carre;
    //private String label;
    
    /*public Bouton(String label){
        super(label);
        addActionListener(this);
    }*/
    
    public Bouton(Case carre){
        this.carre = carre;
        addActionListener(this);

        if (carre.getFixe())
            setBackground(Color.WHITE);
        
        if (carre.getNb() == 0)
            setText(" ");
        else
            setText(carre.toString());
    }
    
    @Override
    public void actionPerformed(ActionEvent e) {
        //boolean fixe = carre.getFixe();
        if (!carre.getFixe()){
            int value = carre.getNb();
            //System.out.println(value);
            if (value == 9){
                setText(" ");
                carre.setNb(0);
            }
            else{
                value++;
                carre.setNb(value);
                setText(carre.toString());
            }
            
            
        }
        else{
            
        }
    }
    
    public void ajouter(){
        
    }
    
}