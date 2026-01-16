/// <reference path="../util.ts" />
/// <reference path="../interfaces.ts" />

module Sudoku {

	export class Move implements BackTrackingSolver.IMove {

		constructor(public point: BackTrackingSolver.Coord, public item: number) { }

		toString(): string {
			return this.point.toFancyString() + ": " + this.item;
		}
	}

	export class State implements BackTrackingSolver.IState {

		items: { [coord: string]: number }

		constructor(public size: number) {
			this.items = {};
		}

		private putLine(s: string): string {
			for (var i = 0; i < this.size * 3 + Math.sqrt(this.size) + 1; i++)
				s += '-';
			return s + "\n";
		}

		toString(): string {
			var cellSize = Math.sqrt(this.size);

			var result: string = "";
			result = this.putLine(result);

			for (var y = 0; y < this.size; y++) {
				result += "|";
				for (var x = 0; x < this.size; x++) {
					var key = x + "x" + y;
					var item = this.items[key]
					result += " " + (item ? (item >= 10 ? item : item + " ") : "  ");

					if ((x + 1) % cellSize == 0) result += "|";

				}

				result += "\n";
				if ((y + 1) % cellSize == 0)
					result = this.putLine(result);			
			}
			return result;
		}

		clone(): State {
			var result = new State(this.size);
			for (var y = 0; y < this.size; y++) {
				for (var x = 0; x < this.size; x++) {
					var key = x + "x" + y;
					var item = this.items[key]
					if (item) result.items[key] = item;
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
					if (!state.items[key]) return false;
				}
			}
			return true;
		}

		isRejectState(state: State): boolean {
			return false;
		}

		apply(state: State, move: Move): State {
			var newState = state.clone();
			newState.items[move.point.toString()] = move.item;
			return newState;
		}

		getPlausibleMoves(state: State): Move[] {
			var result = [];

			for (var y = 0; y < state.size; y++) {
				for (var x = 0; x < state.size; x++) {
					var key = x + "x" + y;
					if (!state.items[key]) {
						for (var item = 1; item <= state.size; item++) {

							var ok = true;

							for (var x2 = 0; x2 < state.size; x2++) {
								if (x2 == x) continue;
								var secondItem = state.items[x2 + "x" + y];
								if (secondItem == item) {
									ok = false;
									break;
								}
							}

							if (!ok) continue;

							for (var y2 = 0; y2 < state.size; y2++) {
								if (y2 == y) continue;
								var secondItem = state.items[x + "x" + y2];
								if (secondItem == item) {
									ok = false;
									break;
								}
							}

							if (!ok) continue;

							var cellSize = Math.sqrt(state.size);
							var cellX = Math.floor(x / cellSize);
							var cellY = Math.floor(y / cellSize);

							for (var y2 = cellSize * cellY; y2 < cellSize * cellY + cellSize; y2++) {
								for (var x2 = cellSize * cellX; x2 < cellSize * cellX + cellSize; x2++) {
									if (x2 == x && y2 == y) continue;
									var secondItem = state.items[x2 + "x" + y2];
									if (secondItem == item) {
										ok = false;
										break;
									}
								}
							}

							if (ok) result.push(new Move(new BackTrackingSolver.Coord(x, y), item));
						}
						return result;
					}
				}
			}

			return result;
		}
	}
}
 