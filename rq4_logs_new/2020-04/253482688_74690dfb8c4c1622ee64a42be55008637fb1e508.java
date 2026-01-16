/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sk.uniza.fri.rex;

import com.badlogic.gdx.Gdx;
import com.badlogic.gdx.graphics.glutils.ShapeRenderer;
import java.util.LinkedList;
import java.util.Random;

/**
 *
 * @author Unlink
 */
public class ManazerPrekazok {
    
    private LinkedList<Prekazka> prekazky = new LinkedList<Prekazka>();
    private Random rnd = new Random();
    
    
    public void posunPrekazky(float delta) {
        //Posun prekazok
        for (Prekazka prekazka : prekazky) {
            prekazka.vykonajPohyb(delta);
        }
        
        //odstranenie starych prekazok
        while (prekazky.size() > 0 && prekazky.getFirst().getX() < 0) {            
            prekazky.removeFirst();
        }
        
        //Pridanie novych
        if (rnd.nextInt() < 10 && (prekazky.size() == 0 || prekazky.getLast().getX() < 200)) {
            prekazky.add(new Prekazka(Gdx.graphics.getWidth(), rnd.nextInt(2) + 1, rnd.nextBoolean()));
        }
    }
    
    public void vykresli(ShapeRenderer shapeRenderer) {
        for (Prekazka prekazka : prekazky) {
            prekazka.vykresli(shapeRenderer);
        }
    }
}