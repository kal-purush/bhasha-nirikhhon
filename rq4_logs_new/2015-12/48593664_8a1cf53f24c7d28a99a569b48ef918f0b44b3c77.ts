import { TTTService as game } from './app.service';

export class TicTacToeAgent {
	agentXorO: string;
	opponentXorO: string;
	
	constructor(xoro: string) {
		this.agentXorO = xoro;
		if (xoro == 'X') this.opponentXorO = 'O';
		else this.opponentXorO = 'X';
	}
	
	private getLegalMoves(state): string[] {
		let legalMoves = [];
		for (let key in state.cellList) {
			if (state.cellList.hasOwnProperty(key)) {
				// Any empty space is a legal move.
				if (!state.cellList[key].xoro) legalMoves.push(key);
			}
		}
		return legalMoves;
	}
	
	private generateSuccessorState(state, move, isMaximizingAgent) {
		if (isMaximizingAgent) state.cellList[move].xoro = this.agentXorO;
		else state.cellList[move].xoro = this.opponentXorO;
		return state;
	}
	
	public getNextMove(stateOrig) {
		let state = JSON.parse(JSON.stringify(stateOrig));
		let moveUtility = this.getUtility(state, true);
		return moveUtility.move;
	}
	
	private getUtility(state, isMaximizingAgent) {
		let gameCondition = game.checkWinCondition(state);
		let moveUtility = {move: null, utility: 0};
		
		// Base cases
		if (gameCondition == 'Win') {
			if (isMaximizingAgent) {
				moveUtility.utility = 1;
				return moveUtility;
			} else {
				moveUtility.utility = -1;
				return moveUtility;
			}
		} else if (gameCondition == 'Draw') return moveUtility;
		
		// Continue recursion
		if (isMaximizingAgent) return this.maxValue(state, isMaximizingAgent);
		else return this.minValue(state, isMaximizingAgent);
	}
	
	private maxValue(state, isMaximizingAgent): any {
		let value = -999999;
		let nextMove = '';
		isMaximizingAgent = !isMaximizingAgent;
		let moveUtility: {move: string, utility: number} = null;
		this.getLegalMoves(state).forEach( move => {
			let nextState = this.generateSuccessorState(state, move, true);
			moveUtility = this.getUtility(nextState, isMaximizingAgent);
			if (moveUtility.utility > value) {
				value = moveUtility.utility;
				nextMove = move;
			}
		});
		moveUtility.utility = value;
		moveUtility.move = nextMove;
		return moveUtility;
	}
	
	private minValue(state, isMaximizingAgent): any {
		let value = 999999;
		let nextMove = '';
		isMaximizingAgent = !isMaximizingAgent;
		let moveUtility: {move: string, utility: number} = null;
		this.getLegalMoves(state).forEach( move => {
			let nextState = this.generateSuccessorState(state, move, false);
			moveUtility = this.getUtility(nextState, isMaximizingAgent);
			if (moveUtility.utility < value) {
				value = moveUtility.utility;
				nextMove = move;
			}
		});
		moveUtility.utility = value;
		moveUtility.move = nextMove;
		return moveUtility;
	}
	
}