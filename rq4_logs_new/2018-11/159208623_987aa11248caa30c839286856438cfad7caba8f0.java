package me.zombies;
import javax.swing.*;

public class Main 
{
	//Global Variables:
	JFrame window;
	DrawingPanel drPanel;
	
	//Window Width and Height:
	final static int winX = 500;
	final static int winY = 500;
	
	public static void main (String [] args)
	{
		new Main();
	}
	
	Main()
	{
		//JFrame Setup:
		window = new JFrame("Zombie Game");
		drPanel = new DrawingPanel();
		
		window.setSize(winX, winY);
		window.setResizable(false);
		window.setLocationRelativeTo(null);
		
		window.add(drPanel);
		window.setVisible(true);
	}
	
	@SuppressWarnings("serial")
	private class DrawingPanel extends JPanel
	{
		DrawingPanel()
		{
			
		}
	}
}