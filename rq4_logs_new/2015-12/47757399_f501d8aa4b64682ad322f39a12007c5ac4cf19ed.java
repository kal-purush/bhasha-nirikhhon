package entity;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

import render.IRenderable;

public class Note implements IRenderable{
	protected int type;
	protected int length;
	protected int line;
	protected int coY,V;
	protected int time;
	protected BufferedImage img;
	public Note(int type,int length, int line, int coY, int V, int time){
		this.type = type;
		this.length = length;
		this.line = line;
		this.coY = coY;
		this.V = V;
		this.time = time;
		try{
			img = ImageIO.read(new File(""));
		}catch(IOException e){e.printStackTrace();}
	}
	
	@Override
	public void draw(Graphics2D g2d) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public boolean isVisible() {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public int getZ() {
		// TODO Auto-generated method stub
		return 0;
	}

}