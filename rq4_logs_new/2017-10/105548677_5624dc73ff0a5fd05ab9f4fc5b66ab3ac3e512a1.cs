using System;
using GXPEngine;

public class Player : AnimationSprite
{
	float ySpeed;
	float xSpeed;
	float xSpeedMax = 5.0f;
	float xSpeedInc = 0.5f;
	float grav = 0.5f;
	float oldYSpeed;
	float oldXSpeed;
	float oldY;
	float oldX;

	int doubleJumpTime;
	float jumpHeight = 8;
	bool hasJumpPowerup;
	bool hasJumped;

	bool movingRight = true;
	bool running = false;

	int animationFrameDelay = 8;
	private int animationStep;

	public Player() : base("player.png", 4, 1)
	{
		x = 100;    //Starting Position X
		y = 600;    //Starting Position Y
	}

	public void ExecuteAnimation()
	{
		if (oldX < x || oldX > x || ySpeed != 0)
		{
			animationStep += 1;
		}
		//Animation based on Movement
		if (animationStep > animationFrameDelay)
		{
			if (ySpeed == 0)	//On the Ground
			{
				SetFrame(1);
			}
			if (ySpeed > 0)		//Falling
			{
				SetFrame(2);
			}
			if (ySpeed < 0)		//Jumping
			{
				SetFrame(3);
			}
			if (ySpeed == 0 && running == true)	//Running
			{

			}
		}
	}

	void Update()
	{
		if (Input.GetKey(Key.D) || Input.GetKey(Key.RIGHT))
		{
			if (xSpeed < xSpeedMax)
			{
				xSpeed += xSpeedInc;
			}
			x += xSpeed;
		}
		//Moing Left
		if (Input.GetKey(Key.A) || Input.GetKey(Key.LEFT))
		{
			if (xSpeed > -xSpeedMax)
			{
				xSpeed -= xSpeedInc;
			}
 			x += xSpeed;	
		}

		ExecuteAnimation();

		//Moving Left
		if (movingRight == false & oldX > x)
		{
			movingRight = true;
			_mirrorX = true;
		}
		//Moving Right
		if (movingRight == true & oldX < x)
		{
			movingRight = false;
			_mirrorX = false; 
		}

		//Double Jumping
		if (doubleJumpTime > 0 && ySpeed != 0 && Input.GetKeyDown(Key.SPACE) && hasJumped == false)
		{
			ySpeed -= jumpHeight;
			hasJumped = true;
		}

		//Save old Stats for later comparison
		oldX = x;
		oldY = y;
		oldXSpeed = xSpeed;
		oldYSpeed = ySpeed;
	}
}
