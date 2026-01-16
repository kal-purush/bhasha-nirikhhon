class Ball
{
	constructor(x,y,width)
	{
		this.x = x;
		this.y = y;
		this.width = width;
		this.killed = false;
		this.bong = false;
	}


	collided()
	{
		if (mouseX >= this.x 
		    && mouseX <= this.x+this.width
		    && mouseY >= this.y
		    && mouseY <= this.y+this.width)
		{
			this.killed = true;
		}
	}
}