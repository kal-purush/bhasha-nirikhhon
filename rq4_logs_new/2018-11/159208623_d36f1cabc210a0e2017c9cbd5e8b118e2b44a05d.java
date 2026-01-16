package me.zombies;


import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.SwingUtilities;


public class GUI extends JFrame {
	public static void main(String[] args) {
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				new GUI();
			}
		});
	} 
	
	GUI(){
		this.add(new DrawPanel());	
		this.setTitle("Main graphics ..."); 
		this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		
		this.pack();
		this.setVisible(true);
	}
	
	class DrawPanel extends JPanel {
					
		int panSize=400;
		
		DrawPanel() {	
			this.setBackground(Color.WHITE);			
			this.setPreferredSize(new Dimension(panSize, panSize));		
		}
		
		@Override
		public void paintComponent(Graphics g) {
			super.paintComponent(g); //clear screen and repaint using background colour
			panSize = this.getWidth();
			Graphics2D g2 = (Graphics2D) g;		
			g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
			
			int r = 20; //circle radius
			
			g.setColor(Color.GREEN.darker());
			g.drawLine(0, panSize/2, panSize/2, panSize/2);
			g.drawLine(panSize/2, panSize/2, panSize/2, panSize);
			g.setColor(Color.BLUE);
			g.drawOval(panSize/2-r/2, panSize/2-r/2, r, r);			
		}
		
		
	}	
}