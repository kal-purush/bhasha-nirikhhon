package Battles;

import Fruit.Fruit;
import Main.Game;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.awt.event.*;

import javax.imageio.ImageIO;
import javax.swing.Timer;

public class Battle implements ActionListener {
    Game gm;
    double defenseMultiplier = 0.2;
    public final int FIGHT = 0;
    public final int RUN = 1;
    public int choice = 2;
    Fruit player;
    Fruit ai;
    public int state;

    public final int entranceState = 0;
    public final int sequenceState = 1;
    public final int dialogueState = 2;
    public final int fightingState = 3;
    public final int healthState = 4;

    public String[] entranceDialogue =  new String[2];
    public String[] fightDialogue = new String[4];
    public String[] healthDialogue = new String[2];

    BufferedImage background = null;

    Graphics2D g;

    String text = "";

    double aiHealth;

    int aiX = 1300;
    int aiY = 100;

    Timer animation;
    int animationCounter = 0;

    public Battle(Game gm){
        this.gm = gm;
        state = 0;
        try {
            background = ImageIO.read(new File("Character Sprites\\Fruits\\BattleBackground.png"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        animation = new Timer(100, this);
    }

    public void instantiateFruits(int i){
        player = new Fruit(gm.fruits[0]);
        ai = new Fruit(gm.fruits[2]);
        state = entranceState;
    }

    public void fightText(){
        fightDialogue[0] = player.name + " used tackle";
        fightDialogue[1] = ai.name + " took " + player.damage + " damage";
        fightDialogue[2] = ai.name + " used tackle";
        fightDialogue[3] = player.name +  " took " + ai.damage + " damage";

        if(gm.keyHandler.fighting == 0){
            text = fightDialogue[0];
        }
        else if(gm.keyHandler.fighting == 1){
            text = fightDialogue[1];
        }
        else if(gm.keyHandler.fighting == 2){
            text = fightDialogue[2];
        }
        else if(gm.keyHandler.fighting == 3){
            text = fightDialogue[3];
        }
        else if(gm.keyHandler.fighting >= fightDialogue.length){
            state = sequenceState;
            gm.keyHandler.fighting = 0;
        }
        
    }

    public void entrance(){
        entranceDialogue[0] = "You have encountered a wild " + ai.name;
        entranceDialogue[1] = "FIGHT!";
        if(gm.keyHandler.entered == 0){
            text = entranceDialogue[0];
        }
        else if(gm.keyHandler.entered == 1){
            text = entranceDialogue[1];
        }
        else if(gm.keyHandler.entered >= entranceDialogue.length){
            state = sequenceState;
            gm.keyHandler.entered = 0;
        }
    }

    public void checkHealth(){
        state = healthState;
        if(ai.currentHealth <= 0){
            text = ai.name + " has fainted";
            return;
        }

        if(player.currentHealth <= 0){
            text = player.name + " has fainted";
            return;
        }

        text = "FIGHT!";
        state = fightingState;
    }

    public void run(){
        gm.gamestate = gm.forestState;
    }

    public void fight(){
        state = fightingState;
        Fruit first;
        Fruit second;
        if(player.speed >= ai.speed){
            first = player;
            second = ai;
        }
        else{
            first = ai;
            second = player;
        }

        if(first == ai){
            animation.start();
            second.currentHealth -= first.damage - (int)(defenseMultiplier * second.defense);
            checkHealth();
            first.currentHealth -= second.damage - (int)(defenseMultiplier * first.defense);
            checkHealth();
        }
    }

    public void paint(Graphics2D g){
        this.g = g;

        int x = 100;
        int y = 700;
        int width = gm.screenWidth - (gm.tileSize*4);
        int height = gm.tileSize*5;
        
        paintBattle();
        if(state == entranceState){
            entrance();
            paintBattleMessage(text);
        }
        else if(state == dialogueState){
            paintBattleMessage(text);
        }
        else if(state == sequenceState){
            playerUI();
        }
        else if(state == fightingState){
            fightText();
            paintBattleMessage(text);
        }
        else if(state == healthState){
            checkHealth();
            paintBattleMessage(text);
        }
    }

    public void playerUI(){
        int x = 100;
        int y = 700;
        int width = gm.screenWidth - (gm.tileSize*4);
        int height = gm.tileSize*5;
        int wordX = gm.screenWidth - gm.tileSize*7;
        int wordY = 800;
        //Player UI Choice
        g.setColor(Color.black);
        g.fillRoundRect(x, y, width, height, 25, 25);

        //Border
        g.setColor(Color.white);
        g.setStroke(new BasicStroke(5));
        g.drawRoundRect(x + 5, y + 5, width-10, height-10, 25, 25);
        
        //Fight
        g.setFont(gm.ui.pixelFont);
        g.setFont(g.getFont().deriveFont(Font.BOLD, 50));
        g.drawString("FIGHT", wordX, wordY);

        //Run
        g.setFont(gm.ui.pixelFont);
        g.setFont(g.getFont().deriveFont(Font.BOLD, 50));
        g.drawString("RUN", wordX, wordY+150);

        if(gm.ui.battleStateChoice == 0){
            //Fight choice
            g.drawString("X", wordX - 100, wordY);
        }
        else{
            //Run Choice
            g.drawString("X", wordX-100, wordY+150);
        }
    }

    public void paintBattle(){
        int wordX = gm.screenWidth - gm.tileSize*7;
        int wordY = 800;

        //Temp Background
        g.setColor(Color.gray);
        g.fillRect(0, 0, gm.screenWidth, gm.screenHeight);
        
        
        //Ai Oval
        g.drawImage(background, 1125, 150, 600, 360, null);
        
        //Ai Character 1300 100
        g.drawImage(ai.image, aiX, aiY, 250, 250, null);
        g.drawString(ai.name, 700, 50);

        //Ai Health Bar
        g.setColor(Color.white);
        g.drawRect(700, 100, 500, 50);

        aiHealth = 500 * ((double)ai.currentHealth / ai.health);
        g.fillRect(700, 100, (int)aiHealth, 50);
    }

    public void paintBattleMessage(String text){
        //Player UI Choice
        g.setColor(Color.black);
        g.fillRoundRect(100, 700, gm.screenWidth - (gm.tileSize*4), gm.tileSize*5, 25, 25);

        //Border
        g.setColor(Color.white);
        g.setStroke(new BasicStroke(5));
        g.drawRoundRect(105, 705, gm.screenWidth - (gm.tileSize*4)-10, gm.tileSize*5-10, 25, 25);

        g.setFont(gm.ui.pixelFont);
        g.setFont(g.getFont().deriveFont(Font.BOLD, 30));
        g.drawString(text, 200, 800);
    }

    @Override
    public void actionPerformed(ActionEvent e) {
        // TODO Auto-generated method stub
        if(animationCounter == 0){
            aiX -= 50;
            animationCounter = 1;
        }
        else{
            aiX += 50;
            animationCounter = 0;
            animation.stop();
        }
    }
    
}