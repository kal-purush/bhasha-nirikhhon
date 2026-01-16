package Lunar_Lander;

import java.awt.Canvas;
import java.awt.Dimension;
import java.awt.Graphics2D;
import java.awt.image.BufferStrategy;
import java.util.ArrayList;
import javax.swing.JFrame;

public class Foundation {
    
    private Canvas game = new Canvas();
    public final static int WIDTH = 1920, HEIGHT = 1080;
    
    private String gameName = "Lunar Lander";
    
    private Input input;
    
    private ArrayList<Updatable> updatables = new ArrayList<>();
    private ArrayList<Renderable> renderables = new ArrayList<>();
    
    public void addUpdatable(Updatable u) {
        updatables.add(u);
    }
    
    public void removeUpdatable(Updatable u) {
        updatables.remove(u);
    }
    
    public void addRenderable(Renderable r) {
        renderables.add(r);
    }
    
    public void removeRenderable(Renderable r) {
        renderables.remove(r);
    }
    
    public void start() {
        Dimension gameSize = new Dimension(WIDTH, HEIGHT);
        JFrame gameWindow = new JFrame(gameName);
        gameWindow.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        gameWindow.setSize(gameSize);
        gameWindow.setResizable(false);
        gameWindow.setVisible(true);
        game.setSize(gameSize);
        game.setMaximumSize(gameSize);
        game.setMinimumSize(gameSize);
        game.setPreferredSize(gameSize);
        gameWindow.add(game);
        gameWindow.setLocationRelativeTo(null);
        
        
        input = new Input();
        game.addKeyListener(input);
        
        final int TICKS_PER_SECOND = 60;
        final int SKIP_TICKS = 1000 / TICKS_PER_SECOND;
        final int MAX_FRAMESKIP = 5;

        long nextGameTick = System.currentTimeMillis();
        int loops;
        float interpolation;
        
        long timeAtLastFPSCheck = 0;
        int ticks = 0;
        
        boolean running = true;
        while(running) {
            loops = 0;
            
            while(System.currentTimeMillis() > nextGameTick && loops < MAX_FRAMESKIP) {
                update();
                ticks++;

                nextGameTick += SKIP_TICKS;
                loops++;
            }
            
            interpolation = (float) (System.currentTimeMillis() + SKIP_TICKS - nextGameTick)
                            / (float) SKIP_TICKS;
            render(interpolation);
            
            if(System.currentTimeMillis() - timeAtLastFPSCheck >= 1000) {
                System.out.println("FPS: " + ticks);
                gameWindow.setTitle(gameName + " - FPS: " + ticks);
                ticks = 0;
                timeAtLastFPSCheck = System.currentTimeMillis();
            }
  
        }
        
    }
    
    private void update() {
        for(Updatable u : updatables) {
            u.update(input);
        }
    }
    
    private void render(float interpolation) {
        BufferStrategy bs = game.getBufferStrategy();
        if(bs == null) {
            game.createBufferStrategy(2);
            return;
        }

        Graphics2D g = (Graphics2D) bs.getDrawGraphics();
        g.clearRect(0, 0, game.getWidth(), game.getHeight());
        for(Renderable r : renderables) {
            r.render(g, interpolation);
        }
        g.dispose();
        bs.show();
    }
}