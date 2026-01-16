/*
 * File: CheckerboardKarel.java
 * ----------------------------
 * When you finish writing it, the CheckerboardKarel class should draw
 * a checkerboard using beepers, as described in Assignment 1.  You
 * should make sure that your program works for all of the sample
 * worlds supplied in the starter folder.
 */

import stanford.karel.*;

public class CheckerboardKarel extends SuperKarel {

	// You fill in this part
	public void run(){

		correctDirectionIfHasOneColumn();
		while(frontIsClear()){
			putBeeper();
			move();
			if(frontIsClear()){
				move();
				correctDirectionIfColumsOdd();
			}
			else if(facingEast()){
				goLeftAndUpAndLeft();
			}
			else if(facingWest()){
				goRightAndUpAndRight();
			}
		}
	}
	
	private void correctDirectionIfHasOneColumn(){
		if(frontIsBlocked()){
			turnLeft();
		}
	}
	
	private void goLeftAndUpAndLeft(){
		turnLeft();
		if(frontIsClear()){
			move();
			turnLeft();
		}
	}
	
	private void goRightAndUpAndRight(){
		turnRight();
		if(frontIsClear()){
			move();
			turnRight();
		}
	}
	
	private void goLeftAndUpAndLeftAndLeft(){
		goLeftAndUpAndLeft();
		if(frontIsClear()){
			move();
		}
	}
	
	private void goRightAndUpAndRightAndRight(){
		goRightAndUpAndRight();
		if(frontIsClear()){
			move();
		}
	}
	
	private void correctDirectionIfColumsOdd(){
		if(frontIsBlocked()){
			putBeeper();
			if(facingEast()){
				goLeftAndUpAndLeftAndLeft();
			}
			else if(facingWest()){
				goRightAndUpAndRightAndRight();
			}
		}
	}
}