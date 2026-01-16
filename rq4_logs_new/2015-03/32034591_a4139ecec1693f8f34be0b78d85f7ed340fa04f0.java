package edu.ycp.cs320.groupProject.model;

public class Paddle {
	Point topleft;
	int width = 125;
	int length = 20;
	
	public Paddle(int x, int y){
		topleft = new Point(x,y);
	}
	public void setTopLeft(Point point){
		topleft = point;
	}
	public void scalePaddle(int percentageValue){
		double t = percentageValue/100;
		width += t*width;
	}public void restorePaddle(){
		width = 125;
	}
	
	//This will only work for rectangle on axis
	public Point findNearest(Ball ball){
		//X values for offset
		int xNearest = 0;
		if((ball.getX() >= topleft.getX() )&& (ball.getX()<=topleft.getX()+width)){
			xNearest = ball.getX();
		}else if(ball.getX() < topleft.getX() ){
			xNearest = topleft.getX();
		}else if(ball.getX() > topleft.getX()+width ){
			xNearest = topleft.getX()+width;
		}
		//Y values for offset
		int yNearest = 0;
		if((ball.getY() >= topleft.getY() )&& (ball.getY()<=topleft.getY()+ length)){
			yNearest = ball.getY();
		}else if(ball.getY() < topleft.getY() ){
			yNearest = topleft.getY();
		}else if(ball.getY() > topleft.getY()+length ){
			yNearest = topleft.getY()+length;
		}
		return new Point(xNearest,yNearest);
	}
	
	public boolean ballCollision(Ball ball){
		Point near = findNearest(ball);
		if(near.distanceTo(new Point(ball.getX(),ball.getY())) <= ball.getRadius()){
			return true;
		}
		return false;
	}
}