/// <reference path="interfaces.ts" />

module BackTrackingSolver {
	export class Solver {

		constructor(private game: IGame) { }

		private solution: IMove[];

		public solve(state: BackTrackingSolver.IState): IMove[] {
			this.solution = undefined;
			this.backTrackingSolve(state, []);
			return this.solution;
		}

		public applySolution(state: BackTrackingSolver.IState, moves: IMove[] ) {
			moves.forEach(function(move) {
				console.log(move.toString());
				state = this.game.apply(state, move);
				console.log(state.toString());
			}, this);
		}

		private backTrackingSolve(state: BackTrackingSolver.IState, currentMoves: IMove[]) {
			
			console.log("Backtracking depth: " + currentMoves.length);

			if (this.game.isRejectState(state)) {
				console.log("Dead end");
				return;
			}
			if (this.game.isAcceptState(state)) {
				this.solution = currentMoves;
				console.log("Got solution :-)");
				return;
			};
			
			var moves = this.game.getPlausibleMoves(state);
			for (var i = 0; i < moves.length; i++) {
				var newState = this.game.apply(state, moves[i]);
				this.backTrackingSolve(newState, currentMoves.concat(moves[i]));
				if (this.solution) return;
			}
		}
	}
}