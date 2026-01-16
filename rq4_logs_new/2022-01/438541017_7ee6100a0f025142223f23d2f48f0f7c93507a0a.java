/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package main;

import model.Case;
import view.page1;

/**
 *
 * @author Administrator
 */
public class run {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
         page1 p = new page1(new Case());
         p.setVisible(true);
         
    }
    
}