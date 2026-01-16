package behavior;

import gameLogic.GameState;
import gameLogic.GameStateManager;
import gameLogic.Move;

import java.util.ArrayList;

public class ComputerWithAI extends MoveStrategy{

	public void makeMove(GameStateManager gsm){
		//my clr
		int myClr=this.getColor();
		int i=0;
		int j=0;
		boolean success = false;
		
		//go thru the possible moves until a good one is made
		while(success==false){
			
			//see if there are any possible jumps
			ArrayList<Move> possibleJumps = gsm.canJump(myClr);
			//see if we can make any of those
			if(possibleJumps.isEmpty()){
				//go thru the board. find a piece, make a possible move.
				outerloop:
				for(i=0; i<8; i++){
					for(j=0; j<8; j++){
						if(gsm.getGameState().getStateOfSquare(i, j)==myClr || gsm.getGameState().getStateOfSquare(i, j)==myClr+2){
							//if we are here, try to make ANY possible move
							//try to go up and to the left
							//if(i+1 = || i-1)
							Move tryThisMove = new Move(i,j,i-1,j+1);
							if(gsm.attemptMove(tryThisMove, myClr)){
								gsm.executeMove(tryThisMove);
								success=true;
								break outerloop;
							}
							//up and to the right
							tryThisMove = new Move(i,j,i+1,j+1);
							if(gsm.attemptMove(tryThisMove, myClr)){
								gsm.executeMove(tryThisMove);
								success=true;
								break outerloop;
							}
							//down and to the left
							tryThisMove = new Move(i,j,i-1,j-1);
							if(gsm.attemptMove(tryThisMove, myClr)){
								gsm.executeMove(tryThisMove);
								success=true;
								break outerloop;
							}
							//down and to the right
							tryThisMove = new Move(i,j,i+1,j-1);
							if(gsm.attemptMove(tryThisMove, myClr)){
								gsm.executeMove(tryThisMove);
								success=true;
								break outerloop;
							}
						}
					}
				}
			}else{ //this part is when we do have a jump we can make
				while(success==false){
					success=gsm.attemptMove(possibleJumps.get(i), myClr);
					i++;
				}
				//finally execute the move
				gsm.executeMove(possibleJumps.get(i-1));
			}
		}
		
	}
	
	
	//this will evaluate the moves
	private int evaluator(GameStateManager gsm){
		//get  the number of pieces and kings
		int myPieces=0;
		int myKings=0;
		int theirPieces=0;
		int theirKings=0;
		
		int myClr = this.getColor();
		int opntClr = 5;
		if(myClr==1){
			opntClr=2;
		}else{
			opntClr=1;
		}
				
		//get the # of kings and pieces
		for(int i=0; i<8; i++){
			for(int j=0; j<8; j++){
				int piece=gsm.getGameState().getStateOfSquare(j, i);
				//increment pieces
				if(piece==myClr){
					myPieces++;
				}else if(piece == myClr+2){
					myKings++;
				}else if(piece == opntClr){
					theirPieces++;
				}else if(piece == opntClr+2){
					theirKings++;
				}
			}
		}
		
		//so this is where the real evaluating comes into play
		return (myPieces*1 + myKings*3)-(theirPieces*1+theirKings*3);
	}
	
	//this will give us back all the possible moves
	private void successor(GameStateManager gsm){
		int[] bestMove= new int[20];
		GameState[] gs = new GameState[20];
	}
}