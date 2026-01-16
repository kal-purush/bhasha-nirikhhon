package entity;

import java.awt.Graphics;

import jernev.GamePanel;

public class Bullet extends Entity{
	int x, y; //LOCATION
	int dx, dy; //ACCELERATION
	
	public void tick(){
		x += dx;
		y += dy;
	}
	
	public void render(GamePanel gPanel, Graphics g){
		gPanel.render(x, y, this, g);
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}