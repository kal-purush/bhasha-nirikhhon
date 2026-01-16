package objects;

import static framework.Game.GAME_TIME;

import org.lwjgl.input.Keyboard;
import org.lwjgl.util.Rectangle;

import framework.Draw;
import framework.Game;
import framework.GameObject;
import framework.ObjectID;

public class Beam extends GameObject{
	
	private int direction;
	
	public Beam(float x, float y, float width, float height, int direction) {
		super(x+100, y+200, width, height, ObjectID.Beam,new Rectangle((int)(x),(int)(y+height/2.45),(int)width,(int)(height/6.1)));
		this.direction = direction;
		xSpeed = 25;
	}

	@Override
	public void tick() {

	}

	public void moveBeam(){
		x += xSpeed * direction;
	}
	public void collision()
	{
		//nothing
	}
	@Override
	public void render() {
		Draw.drawQuad(x, y, width, height,1,0,0);
		moveBeam();
	}
}