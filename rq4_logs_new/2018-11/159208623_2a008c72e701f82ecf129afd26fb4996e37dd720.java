package me.zombies;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.Timer;

public class PlayerMove implements KeyListener {

//Global Variables	
	Color white = new Color (255, 255, 255);
	Color blue = new Color (0, 0, 255);
	Color red = new Color (255, 0, 0);
	
	final static int WIN = 1500;
	DrawingPanel drPanel = new DrawingPanel();
	static JFrame window;
	static PlayerStats Player = new PlayerStats("Josh");
	
	Timer timer;
	int tSpeed = 5;
	
	public static void main(String[] args) {new PlayerMove();}
	
	PlayerMove() {
	
		window = new JFrame("Player");
		window.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		window.setResizable(false);	
		drPanel.addKeyListener(this);
		window.add(drPanel); 
		window.pack();	
		window.setVisible(true);
		
		timer = new Timer(tSpeed, new TimerListener());
		timer.start();
	}
	
	@SuppressWarnings("serial")
	private class DrawingPanel extends JPanel {
		
		DrawingPanel(){
			this.setPreferredSize(new Dimension (WIN, WIN));
			this.setBackground(white);
		}
		
		@Override
		public void paintComponent(Graphics g) {
			super.paintComponent(g);
			this.requestFocus();
			
			g.setColor(blue);
			g.fillRect(PlayerStats.x, PlayerStats.y, 100, 100);
		}
	}
	

	@Override
	public void keyPressed(KeyEvent e) {
		
		if (e.getKeyCode()==KeyEvent.VK_W) Player.up();
		if (e.getKeyCode()==KeyEvent.VK_A) Player.left();
		if (e.getKeyCode()==KeyEvent.VK_S) Player.down();
		if (e.getKeyCode()==KeyEvent.VK_D) Player.right();
		
	}
	
	private class TimerListener implements ActionListener{

		@Override
		public void actionPerformed(ActionEvent arg0) {
			// TODO Auto-generated method stub
			
		}
		
	}


//UNUSED METHODS
	public void keyReleased(KeyEvent arg0) {}
	public void keyTyped(KeyEvent arg0) {}
}