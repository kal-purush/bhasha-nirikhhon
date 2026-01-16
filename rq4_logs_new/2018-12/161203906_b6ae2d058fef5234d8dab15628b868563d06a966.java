/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bandeau;

import java.awt.Color;
import java.awt.Font;
import java.util.HashSet;

/**
 *
 * @author infoo
 */
public class ExempleScenario {

    HashSet<Effect> mlisteE = new HashSet();
    private final Scenario monScenario = new Scenario(mlisteE, 2);
    
    public static void main(String[] args) {
        // TODO code application logic here
        new ExempleScenario().Test();
    }
    public void Test(){
    
        Effect e1 = new Flash("Flash");
        Effect e2 = new ClockAndAnticlock("Tourner et detourner");
        Effect e3 = new Divide("DIVISER");
        Effect e4 = new Rainbow("On change de couleur");
        
        monScenario.playOn();
        
    }
    
}