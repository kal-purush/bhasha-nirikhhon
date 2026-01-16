package com.testeja.main;

import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.Random;
import com.testeja.main.Game.State;

public class Menu  extends MouseAdapter{
	
	private Game game;
	private Handler handler;
	private Random r = new Random();
	
	public Menu(Game game, Handler handler){
		this.game = game;
		this.handler = handler;
		
	}
	
	public void mousePressed(MouseEvent e){
		int mx = e.getX();
		int my = e.getY();
		
		if(game.gameState == State.Menu){
		//play button
		if(mouseOver(mx, my, 475, 200, 250, 100)){
			game.gameState = State.game;
			handler.addObject(new Player(Game.WIDTH/2-32, Game.HEIGHT/2-32, ID.Player1, handler));
			handler.addObject(new BasicEnemy(r.nextInt(Game.WIDTH),r.nextInt(Game.HEIGHT), ID.BasicEnemy, handler));
			handler.addObject(new BasicEnemy(r.nextInt(Game.WIDTH), r.nextInt(Game.HEIGHT), ID.BasicEnemy, handler));
		}
		//help button
		if(mouseOver(mx, my, 50, 25, 200, 75)){
			game.gameState = State.Help;
		}

		//back button for help
		if(game.gameState == State.Help){
			if(mouseOver(mx, my,475, 450, 250, 100));
			game.gameState = State.Menu;
			return;
	   }
		//quit button
		if(mouseOver(mx, my, 475, 600, 250, 100)){
			System.exit(0);
		}
	  }
	}	
	
	public void mouseReleased(MouseEvent e){
		
	}
	
	private boolean mouseOver(int mx, int my, int x, int y, int width, int height){
		if(mx > x && mx < x + width){
			if(my > y && my < y + height){
				return true;
			}else return false;	
		}else return false;
	}	
	public void tick(){
		
	}
	public void render(Graphics g){
		if(game.gameState == State.Menu){
		
		Font fnt = new Font("arial", 1, 50);
		Font fnt2 = new Font("arial", 1, 35);
		
		g.setFont(fnt);
		g.setColor(Color.WHITE);
		g.drawString("Main Menu", 475, 125);
		
		g.setFont(fnt2);
		g.drawRect(475, 200, 250, 100);
		g.setColor(Color.RED);
		g.drawString("Play", 560, 265);
		
		
		g.setColor(Color.WHITE);
		g.drawRect(475, 400, 250, 100);
		g.setColor(Color.GREEN);
		g.drawString("Top Scores", 510, 465);
		
		
		g.setColor(Color.WHITE);
		g.drawRect(475, 600, 250, 100);
		g.setColor(Color.CYAN);
		g.drawString("Quit", 560, 665);
		
		g.setColor(Color.WHITE);
		g.drawRect(50, 25, 200, 75);
		g.setColor(Color.CYAN);
		g.drawString("Help", 110, 75);
		
		}else if(game.gameState == State.Help){
			
			Font fnt = new Font("arial", 1, 50);
			
			g.setFont(fnt);
			g.setColor(Color.WHITE);
			g.drawString("Help", 475, 125);
			g.drawString("Talo palaa lol apua.", 300, 250);
			
			g.drawRect(475, 450, 250, 100);
			g.setColor(Color.WHITE);
			g.drawString("Back", 525, 515);
		}
	}
}