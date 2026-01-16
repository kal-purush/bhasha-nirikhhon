package jessi;

import java.awt.Color;
import java.awt.Graphics2D;

import guiTeacher.components.Action;
import guiTeacher.components.Button;

public class CustomButton extends Button {
	private String s1;
	private String s2;

	public CustomButton(int x, int y) {
		super(x, y, w, h, text, null);
		// TODO Auto-generated constructor stub
	}
	
	public void drawButton(Graphics2D g, boolean hover) {
		g.setColor(Color.black);
		g.drawString(s1, 20, 20);
		g.drawString(s2, 20, 30);
		
		g.setColor(Color.red);
		g.drawRect(20, 20, 40, 50);
	}
	
	public void 

}