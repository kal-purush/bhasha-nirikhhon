/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sudoku;

import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.TextArea;
import java.io.IOException;
import javax.swing.JEditorPane;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;

/**
 *
 * @author Clément
 */
public class Aide extends JFrame {
    
    public Aide() {
        this.setTitle("Règles");
        this.setSize(300,250);
        this.setMinimumSize(new Dimension(300, 250));
        this.setMaximumSize(new Dimension(300, 250));
        this.setLocationRelativeTo(null);
        JLabel text = new JLabel("<html><h2> Règles du Sudoku : </h2><br></br><p>Un sudoku classique contient neuf lignes et neuf colonnes, donc 81 cases au total. Le but du jeu est de remplir ces cases avec des chiffres allant de 1 à 9 en veillant toujours à ce qu'un même chiffre ne figure qu'une seule fois par colonne, une seule fois par ligne, et une seule fois par carré de neuf cases.</p></html>");
        this.add(text);
        setVisible(true);
        
    }
    
        
    
}