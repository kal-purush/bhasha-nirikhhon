/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bitzgame.bitz;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;
import org.newdawn.slick.AppGameContainer;
import org.newdawn.slick.BasicGame;
import org.newdawn.slick.GameContainer;
import org.newdawn.slick.Graphics;
import org.newdawn.slick.SlickException;

/**
 *
 * @author Pyry Kontio
*/

class OkResult extends Throwable {
    
}

class GameTestCtx extends BasicGame implements Runnable {

    static AppGameContainer container;
    static BlockingQueue<Supplier<Throwable>> inputQueue;
    static BlockingQueue<Throwable> returnQueue;
    static OkResult ok;

    public GameTestCtx(String title) {
        super(title);
    }

    @Override
    public void init(GameContainer gc) throws SlickException {
    }

    @Override
    public void update(GameContainer gc, int i) throws SlickException {
        Supplier<Throwable> test;
        try {
            System.out.println("UPDATE 1");
            test = inputQueue.take();
            System.out.println("UPDATE 2 taken");
            Throwable result = test.get();
            System.out.println("UPDATE 3 test ran");
            returnQueue.put(result);
            System.out.println("UPDATE 4 put");
        } catch (InterruptedException ex) {}

    }

    @Override
    public void render(GameContainer gc, Graphics grphcs) throws SlickException {
    }

    @Override
    public void run() {
        try {
            container = new AppGameContainer(this);
            container.setDisplayMode(640, 480, false);
            container.start();

        } catch (SlickException ex) {
        }
    }

    public static void setup() {
        GameTestCtx.ok = new OkResult();
        GameTestCtx.inputQueue = new ArrayBlockingQueue<Supplier<Throwable>>(1);
        GameTestCtx.returnQueue = new ArrayBlockingQueue<Throwable>(1);
        (new Thread(new GameTestCtx("TestCtx"))).start();
    }
}