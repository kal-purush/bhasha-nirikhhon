/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Examples.PingPong;

import java.awt.Color;
import java.awt.Point;
import java.awt.geom.Ellipse2D;
import java.util.Random;
import java2dscrollinguniverse.Model.TwoDimensionalMovement;
import java2dscrollinguniverse.Model.actors.Actor;
import java2dscrollinguniverse.Model.actors.ActorLabel;
import java2dscrollinguniverse.Model.actors.ActorLabel.PositionOfLabel;
import java2dscrollinguniverse.Model.actors.ActorType;

/**
 *
 * @author andrewnyhus
 */
public class Ball extends Actor{
        
        private Point initialPoint;
        private Actor leftScoreBox;
        private Actor rightScoreBox;
        private int leftScore = 0, rightScore = 0;
        
        public Ball(Point p, Actor leftScoreBox, Actor rightScoreBox){
            super(ActorType.miscObject, p, Color.white);
            this.setShouldCheckCollisions(true);

           this.initialPoint = p;
           
           this.leftScoreBox = leftScoreBox;
           this.rightScoreBox = rightScoreBox;
           this.updateScoreBoxes();
           setRandomInitialVelocity();

           double ballDiameter = 20.0;
           Ellipse2D.Double ballShape = new Ellipse2D.Double(0.0, 0.0, ballDiameter, ballDiameter);
           this.setShape(ballShape);
           this.setIdString("Ball");

        }

        
        private void updateScoreBoxes(){
            this.leftScoreBox.setActorLabel(new ActorLabel(this.leftScore + "",PositionOfLabel.MIDDLE_OF_BOTTOM));
            this.rightScoreBox.setActorLabel(new ActorLabel(this.rightScore + "",PositionOfLabel.MIDDLE_OF_BOTTOM));
        }
        
        private void setInitialVelocityWithDirection(boolean goRight){
            int initialBallSpeedX = 2;

            if(!goRight){
                initialBallSpeedX *= -1;
            }
            
            Random gen = new Random();
            int initialBallSpeedY = gen.nextInt(6) - 3;
            if (initialBallSpeedY == 0)
                initialBallSpeedY = 1;
            
            TwoDimensionalMovement initialBallVelocity = new TwoDimensionalMovement(initialBallSpeedX, initialBallSpeedY);
            this.setVelocity(initialBallVelocity);
        }
        
        private void setRandomInitialVelocity() {
            Random gen = new Random();
            
            int initialBallSpeedX = 2;
            
            if (gen.nextBoolean() == true){
                //go left
                initialBallSpeedX = -1*initialBallSpeedX;
            }
            
            int initialBallSpeedY = gen.nextInt(6) - 3;
            if (initialBallSpeedY == 0)
                initialBallSpeedY = 1;
            
            TwoDimensionalMovement initialBallVelocity = new TwoDimensionalMovement(initialBallSpeedX, initialBallSpeedY);
            this.setVelocity(initialBallVelocity);
        }

        @Override
        public void handleCollisionWithActor(Actor aCollided){

            switch(aCollided.getIdString()){

                case "PADDLE_LEFT":
                    this.getVelocity().setXMovement(this.getVelocity().getXMovement() * (-1) );
                    break;

                case "PADDLE_RIGHT":
                    this.getVelocity().setXMovement(this.getVelocity().getXMovement() * (-1) );
                    break;

                case "LEFT_EDGE":

                    this.rightScore += 1;
                    this.updateScoreBoxes();

                    this.setTopLeftLocation(this.initialPoint);//put at initial point
                    this.setInitialVelocityWithDirection(false);
                    
                    
                    break;

                case "RIGHT_EDGE":

                    this.leftScore += 1;

                    this.updateScoreBoxes();

                    this.setTopLeftLocation(this.initialPoint);//put at initial point
                    this.setInitialVelocityWithDirection(true);
                    
                    break;
                    
                default:
                    break;
            }
        }

    }