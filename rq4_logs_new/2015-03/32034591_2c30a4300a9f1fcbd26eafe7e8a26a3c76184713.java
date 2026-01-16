package edu.ycp.cs320.groupProject.webapp.shared.controller;

import edu.ycp.cs320.groupProject.webapp.shared.model.Paddle;
import edu.ycp.cs320.groupProject.webapp.shared.model.Point;
import edu.ycp.cs320.groupProject.webapp.shared.model.Stage;

public class PlayerController{
	private boolean moveLeft, moveRight;
	Paddle ownedPaddle;
	
	PlayerController(Paddle paddle){
		ownedPaddle = paddle;
	}
	
	public void moveRight(){
		if(ownedPaddle.isVertical()){
			if(ownedPaddle.getTopLeft().getY()+ ownedPaddle.getWidth() < Stage.HEIGHT){
				ownedPaddle.setTopLeft(new Point(ownedPaddle.getTopLeft().getX(),ownedPaddle.getTopLeft().getY()+ownedPaddle.getSpeed()));
			}
		}else{
			if(ownedPaddle.getTopLeft().getX()+ ownedPaddle.getWidth()< Stage.WIDTH){
				ownedPaddle.setTopLeft(new Point(ownedPaddle.getTopLeft().getX()+ownedPaddle.getSpeed(),ownedPaddle.getTopLeft().getY()));
			}
		}
	}
	
	public void moveLeft(){
		if(ownedPaddle.isVertical()){
			if(ownedPaddle.getTopLeft().getY() > 0){
				ownedPaddle.setTopLeft(new Point(ownedPaddle.getTopLeft().getX(),ownedPaddle.getTopLeft().getY()-ownedPaddle.getSpeed()));
			}
		}else{
			if(ownedPaddle.getTopLeft().getX() > 0){
				ownedPaddle.setTopLeft(new Point(ownedPaddle.getTopLeft().getX()-ownedPaddle.getSpeed(),ownedPaddle.getTopLeft().getY()));
			}
		}
	}
}