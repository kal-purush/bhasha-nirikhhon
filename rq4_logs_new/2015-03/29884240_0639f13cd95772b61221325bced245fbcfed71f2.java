package com.groupc.screens;

import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;

import javax.swing.JButton;
import javax.swing.JLabel;

import com.groupc.Runner;
import com.groupc.math.Resize;

public class GameHubScreen extends Screen
{
	//Variables
	boolean redraw		= true;
	Resize  resize		= new Resize(run);
	int		butPressed	= 0;
	boolean	played		= true;
	
	//Display Elements
	NavigationBar 	navBar		= new NavigationBar(run,false,true,"Game Hub");	
	JButton 		startButton				= new JButton("Game 1");
	
	public GameHubScreen(Runner run)
	{
		super(run);
		//Basic Frame Settings
		//run.setTitle("Recording");
		
		//resize stuff
		run.addComponentListener(new ComponentAdapter()
		{
			public void componentResized(ComponentEvent e)
			{
				redraw = true;
			}
		});
		
		////Buttons////
		
		startButton.addActionListener(new ActionListener()
		{
			@Override
			public void actionPerformed(ActionEvent arg0) {
				butPressed = 4;
			}
		});		
		
		this.add(startButton);
		this.add(navBar);
		
		this.setLayout(null);		
		run.setContentPane(this);
		run.setVisible(true);		
	}
	
	@Override
	public void update(float deltaTime)
	{
		if(redraw)
		{
			//navBar
			navBar.redrawUpdate();
			
			//startButton
			startButton.setBounds(resize.locationX(250), resize.locationY(190), resize.width(75), resize.height(25));
			startButton.setFont(new Font(startButton.getFont().getFontName(),startButton.getFont().getStyle(), resize.font(12)));	
			
			run.repaint();
			redraw = false;
		}
		else if(butPressed == 4)
		{
			run.setScreen(new Game1Screen(run));
		}
		butPressed = 0;
		navBar.update();
	}

	@Override
	public void present(float deltaTime) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void pause() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void resume() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void dispose() 
	{
		// TODO Auto-generated method stub
	}
}