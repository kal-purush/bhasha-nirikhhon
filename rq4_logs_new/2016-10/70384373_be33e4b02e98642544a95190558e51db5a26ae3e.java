import java.awt.Graphics;
import java.awt.Image;
import java.net.URL;


public class Ball {
	
	private int x = 200, y = 50;
	private int radius = 20;
	private double dx = 0, dy = 0;
	private double dt = .2;
	private int gravity = 15;
	private double GameDy = 80;
	private boolean GM;
	URL url;
	static Image ball;
	static MainProject sp;
	
	

	public int getGravity() {
		return gravity;
	}

	public void setGravity(int gravity) {
		this.gravity = gravity;
	}

	public int getX() {
		return x;
	}

	public void setX(int x) {
		this.x = x;
	}

	public int getY() {
		return y;
	}

	public void setY(int y) {
		this.y = y;
	}

	public int getRadius() {
		return radius;
	}

	public void setRadius(int radius) {
		this.radius = radius;
	}

	public double getDx() {
		return dx;
	}

	public void setDx(int dx) {
		this.dx = dx;
	}

	public double getDy() {
		return dy;
	}

	public void setDy(double dy2) {
		this.dy = dy2;
	}

	public double getGameDy() {
		return GameDy;
	}

	public void setGameDy(int gameDy) {
		GameDy = gameDy;
	}

	public Ball() {
		
	}
	
	public Ball(int x, int y){
		this.x = x;
		this.y = y;
	}
	
	public void RightMove(){
		if (dx < 20){
			dx += 2.5;
		}
	}
	
	public void LeftMove(){
		if (dx > -20){
			dx -= 2.5;
		}
	}
	
	public void update (MainProject sp){
		try {
		url = sp.getDocumentBase();
		} catch (Exception e){
			
		}
		ball = sp.getImage(url, "Images/Hero (2).png");
		if (x + dx> sp.getWidth()){
			x = sp.getWidth();
			dx = -dx;
		}else if(x + dx< 0 ){
			x = 0;
			dx = -dx;
		}else {
			x += dx;
		}
		
		if (y + dy > sp.getHeight() -25){
			y = sp.getHeight()-25;
			dy = - GameDy;
		}else if(y + dy< 0){
			y = 0;
			dy = -dy;
		}else {	
			dy += gravity*dt;
			y += dy*dt +0.5*gravity*dt*dt;
		}
		
		if (y+radius > sp.getHeight() -30){
			GM = true;
		}
		
		this.sp = sp;
	}
	
	public void paint(Graphics g){
		
		g.drawImage(ball, x, y, sp);
		
	}

	public boolean getGameOver() {
		// TODO Auto-generated method stub
		return GM;
	}
	

}