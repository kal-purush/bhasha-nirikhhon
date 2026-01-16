/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bitzgame.bitz;

import java.util.ArrayList;

import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;
import org.newdawn.slick.Input;
import org.newdawn.slick.SlickException;
//import org.newdawn.slick.Input;

/**
 *
 * @author Timo
 */

public class GameObjectTest {

    public GameObjectTest() {
    }
    
    GameTestCtx gameLoop;

    @Before
    public void setUp() throws SlickException {
        gameLoop = GameTestCtx.setup();
    }

    @Test
    public void gameObjectRenderableTest1() throws InterruptedException {
        
        Throwable result = gameLoop.runInLoop(() -> {
            try {
                GameObject g = new GameObject(0,0);
                Camera c = new Camera(0,0);
                if(!g.renderable(c)){
                    throw new Exception();
                } 
                return new OkResult();
            } catch (Exception e) {
                return e;
            }
        });
        
        assertEquals("Did it work?", new OkResult(), result);
    }
    @Test
    public void gameObjectRenderableTest2() throws InterruptedException {
        
        Throwable result = gameLoop.runInLoop(() -> {
            try {
                GameObject g = new GameObject(0,0);
                Camera c = null;
                if(g.renderable(c)){
                    return new ErrMsg("Object shouldn't be renderable with a Camera that is null!");
                } 
                return new OkResult();
            } catch (Exception e) {
                return e;
            }
        });
        
        assertEquals("Did it work?", new OkResult(), result);
    }
    
    @Test
    public void gameObjectCollidesWithTest() throws InterruptedException {
        Throwable result = gameLoop.runInLoop(() -> {
            try {
                GameObject g = new GameObject(0,0);
                GameObject gg = new GameObject(1,1);
                if(g.collidesWith(gg)){
                    return new OkResult();
                } else {
                    return new ErrMsg("Doesnt collide");
                }
            } catch (Exception e) {
                return e;
            }
        });
        
        assertEquals("Did it work?", new OkResult(), result);
    }
    
    @Test
    public void gameObjectCollidesAnyTest() throws InterruptedException {
        
        Throwable result = gameLoop.runInLoop(() -> {
            try {
                GameObject g = new GameObject(0,0);
                GameObject gg = new GameObject(1,1);
                Projectile p = new Projectile(15,15,"src/assets/spr_item_crest.png",3);
                ArrayList<GameObject> list = new ArrayList<GameObject>();
                list.add(gg);
                list.add(p);
                if(g.collidesAny(list)!=null){
                    return new OkResult();
                } else {
                    return new ErrMsg("Doesn't collide with anything");
                }
            } catch (Exception e) {
                return e;
            }
        });
        
        assertEquals("Did it work?", new OkResult(), result);
    }
    
    @Test
    public void playerShootTest() throws InterruptedException {
        
        Throwable result = gameLoop.runInLoop(() -> {
            try {
                Input i = new Input(480);
                Player p = new Player(0,0,"src/assets/spr_item_crest.png",5,i);
                p.setTryShoot(true);
                if (p.shoot(2) != null) {
                    return new OkResult();
                } else {
                    return new ErrMsg("Nothing shot");
                }
                
            } catch (Exception e) {
                return e;
            }
        });
        
        assertEquals("Did it work?", new OkResult(), result);
    }
    
    
    

}