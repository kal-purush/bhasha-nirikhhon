package denaro.nick.roguelite;

import java.awt.Point;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseMotionListener;

import denaro.nick.core.EngineAction;
import denaro.nick.core.GameEngine;
import denaro.nick.core.view.GameView2D;

public class DisplayUpdater implements KeyListener, MouseMotionListener
{
	
	@Override
	public void mouseDragged(MouseEvent e)
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void mouseMoved(MouseEvent me)
	{
		WorldView view = (WorldView) GameEngine.instance().view();
		
		GameEngine.instance().addAction(new EngineAction(){

			@Override
			public void init()
			{
			}

			@Override
			public void callFunction()
			{
				Point p = me.getPoint();
				view.viewPos = new Point(p.x - view.width() / 2 / 16, p.y - view.height() / 2 / 16);
			}

			@Override
			public boolean shouldEnd()
			{
				return true;
			}
			
		});
	}

	@Override
	public void keyTyped(KeyEvent e)
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void keyPressed(KeyEvent ke)
	{
	}

	@Override
	public void keyReleased(KeyEvent e)
	{
		// TODO Auto-generated method stub
		
	}

}