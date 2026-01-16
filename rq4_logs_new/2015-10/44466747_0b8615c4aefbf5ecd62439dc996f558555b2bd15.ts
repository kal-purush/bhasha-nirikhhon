/// <reference path="../util.ts" />
/// <reference path="../interfaces.ts" />

module Harmony {

	export class Move implements BackTrackingSolver.IMove {

		constructor(public point1: BackTrackingSolver.Coord, public point2: BackTrackingSolver.Coord) {}

		toString(): string {
			return this.point1.toString() + " <-> " + this.point2.toString();
		}
	}

	interface ICell {
		line: number;
		dots: number;
	}

	export class State implements BackTrackingSolver.IState {

		colors: { [coord: string]: ICell }

		constructor(public size: number) {
			this.colors = {};
		}		

		toString(): string {
			var result: string = "";
			for (var y = 0; y < this.size; y++) {
				for (var x = 0; x < this.size; x++) {
					var cell = this.colors[x + "x" + y];
					result += cell.line + "(" + cell.dots + ") ";
				}
				result += "\n";
			}
			return result;
		}

		clone(): State {
			var result = new State(this.size);

			for (var y = 0; y < this.size; y++) {
				for (var x = 0; x < this.size; x++) {
					var key = x + "x" + y;
					var cell = this.colors[key];
					result.colors[key] = { line: cell.line, dots: cell.dots };
				}
			}
			return result;
		}
	}

	export class Game implements BackTrackingSolver.IGame {

		isAcceptState(state: State): boolean {
			for (var y = 0; y < state.size; y++) {
				for (var x = 0; x < state.size; x++) {
					var key = x + "x" + y;
					var cell = state.colors[key];
					if ((cell.dots > 0) || (y != cell.line)) return false;
				}
			}
			return true;
		}

		isRejectState(state: State): boolean {
			for (var y = 0; y < state.size; y++) {
				for (var x = 0; x < state.size; x++) {
					var key = x + "x" + y;
					var cell = state.colors[key];
					if (y != cell.line && cell.dots == 0) return true;
				}
			}
			return false;
		}

		apply(state: State, move: Move): State {
			var newState = state.clone();

			var p1 = move.point1.toString();
			var p2 = move.point2.toString();
			newState.colors[p1].dots--;
			newState.colors[p2].dots--;

			var tmp = newState.colors[p1].line;
			newState.colors[p1].line = newState.colors[p2].line;
			newState.colors[p2].line = tmp;

			return newState;
		}

		getPlausibleMoves(state: State): Move[] {
			var result = [];

			for (var y = 0; y < state.size; y++) {
				for (var x = 0; x < state.size; x++) {

					var key1 = x + "x" + y;
					var cell1 = state.colors[key1];
					if (cell1.dots == 0) continue;

					for (var x2 = x + 1; x2 < state.size; x2++) {
						var key2 = x2 + "x" + y;
						var cell2 = state.colors[key2];
						if (cell2.dots > 0)
							result.push(new Move(new BackTrackingSolver.Coord(x, y), new BackTrackingSolver.Coord(x2, y)));
					}

					for (var y2 = y + 1; y2 < state.size; y2++) {
						var key2 = x + "x" + y2;
						var cell2 = state.colors[key2];
						if (cell2.dots > 0)
							result.push(new Move(new BackTrackingSolver.Coord(x, y), new BackTrackingSolver.Coord(x, y2)));
					}
				}
			}

			return result;
		}
	}
}
 