package me.zombies;

//Java Imports:
import javax.swing.*;
import java.awt.*;

//Map Imports:
import maps.*;
import props.*;

public class Main 
{
	//Global Variables:
	JFrame window;
	DrawingPanel drPanel;
	
	//Window Width and Height:
	final static int winX = 500;
	final static int winY = 500;
	
	//Custom Colors:
	static Color STREETCOLOUR = new Color(146, 146, 146);
	
	//Maps:
	TestMap testMap = new TestMap();
	
	public static void main (String [] args)
	{
		new Main();
	}
	
	Main()
	{
		//JFrame Setup:
		window = new JFrame("Zombie Game");
		drPanel = new DrawingPanel();
		testMap.addProps();
		
		System.out.println("Buildings: "+testMap.buildings.size());
		System.out.println("Trees: "+testMap.trees.size());
		
		window.setSize(winX, winY);
		window.setResizable(false);
		window.setLocationRelativeTo(null);
		window.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		
		window.add(drPanel);
		window.setVisible(true);
	}
	
	@SuppressWarnings("serial")
	private class DrawingPanel extends JPanel
	{
		DrawingPanel()
		{
			this.setBackground(STREETCOLOUR);
		}
		
		public void paintComponent(Graphics g)
		{
			super.paintComponent(g);
			
			for (Building b : testMap.buildings)
			{
				b.paint(g);
			}
			
			for (Tree t : testMap.trees)
			{
				t.paint(g);
			}
		}
	}
}