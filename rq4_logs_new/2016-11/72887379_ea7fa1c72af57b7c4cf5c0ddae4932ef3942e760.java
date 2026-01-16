/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package caixeiroviajanteguloso;

import javax.swing.JFrame;
import javax.swing.JOptionPane;

/**
 *
 * @author Tarcisio
 */
public class CaixeiroViajanteGuloso {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Pontos panel = new Pontos(20,800,600);
        JFrame application = new JFrame("Resultado");
        application.add(panel);
        application.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        application.setSize(500,300);
        application.setVisible(true);
        
    }
    
}