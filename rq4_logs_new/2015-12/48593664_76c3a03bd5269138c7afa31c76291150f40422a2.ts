import { Injectable } from 'angular2/core';

@Injectable()
export class TTTService {

	svgs = {
		["X"]: '../assets/tictactoe-x.svg',
		["O"]: '../assets/tictactoe-o.svg'
	}
	turnText = '';

	gameState = {
		currTurn: "X",
		winner: '',
		cellList: {
			['cell1']: { xoro: '', anim: null }, ['cell2']: { xoro: '', anim: null }, ['cell3']: { xoro: '', anim: null },
			['cell4']: { xoro: '', anim: null }, ['cell5']: { xoro: '', anim: null }, ['cell6']: { xoro: '', anim: null },
			['cell7']: { xoro: '', anim: null }, ['cell8']: { xoro: '', anim: null }, ['cell9']: { xoro: '', anim: null }
		}
	}
	
	constructor() {
		this.setTurnText();
	}
	
	setTurnText() {
		if (!this.gameState.winner) this.turnText = `Next move: ${this.gameState.currTurn}`;
		else `${this.gameState.winner} Wins!!`; 
	}
	
	setAnimation(cellID: string, svgFile: string) {
		return new Vivus(cellID, {duration: 25, file: svgFile, start: 'manual', onReady: this.readyAnimate});
	}
	
	readyAnimate(vivusAnim) {
		vivusAnim.play(1);
	}
	
	animate(cell) {
		let clickedCell = this.gameState.cellList[cell.id];
		if (clickedCell.anim == null) {
			clickedCell.anim = this.setAnimation(cell.id, this.svgs[this.gameState.currTurn]);
			clickedCell.xoro = this.gameState.currTurn
			if (this.checkWinCondition) {
				this.gameState.winner = this.gameState.currTurn;
				this.setTurnText();
			} else this.nextTurn();
		}
	}
	
	nextTurn() {
		if (this.gameState.currTurn == "X") this.gameState.currTurn = "O";
		else this.gameState.currTurn = "X";
	}
	
	getMoveAt(position: string): string {
		return this.gameState.cellList[`cell${position}`].xoro;
	}
	
	checkRow(pos1: string, pos2: string, pos3:string): boolean {
		if (!this.getMoveAt(pos1)) return false;
		if (this.getMoveAt(pos1) == this.getMoveAt(pos2) &&
			this.getMoveAt(pos2) == this.getMoveAt(pos3)) return true;
		else return false;
	}
	
	checkWinCondition() {
		// Horizontal Wins
		if (this.checkRow('1', '2', '3')) return true;
		if (this.checkRow('4', '5', '6')) return true;
		if (this.checkRow('7', '8', '9')) return true;
		
		// Vertical Wins
		if (this.checkRow('1', '4', '7')) return true;
		if (this.checkRow('2', '5', '8')) return true;
		if (this.checkRow('3', '6', '9')) return true;
		
		// Diagonal Wins 
		if (this.checkRow('1', '5', '9')) return true;
		if (this.checkRow('3', '5', '7')) return true;
		
		return false;
	}
}