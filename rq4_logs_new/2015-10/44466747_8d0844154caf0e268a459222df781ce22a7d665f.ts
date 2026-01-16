/// <reference path="../util.ts" />
/// <reference path="../interfaces.ts" />

module Puralax {

	export enum Colors {
		Gray, Red, Green, Yellow, Blue, Purple
	}

	export class Move implements BackTrackingSolver.IMove {

		constructor(public point: BackTrackingSolver.Coord, public direction: BackTrackingSolver.Direction) {}

		toString(): string {
			return this.point.toString() + ": " + direction2char(this.direction);
		}
	}

	interface ICell {
		color: Colors;
		dots: number;
	}

	function color2char(color: Colors) {
		switch (color) {
			case Colors.Gray:
				return " ";
			case Colors.Red:
				return "R";
			case Colors.Green:
				return "G";
			case Colors.Yellow:
				return "Y";
			case Colors.Blue:
				return "B";
			case Colors.Purple:
				return "P"; 
		}
		throw "Uknown color: " + color;
	}

	function direction2char(direction: BackTrackingSolver.Direction) {
		switch (direction) {
			case BackTrackingSolver.Direction.Up:
				return "^";
			case BackTrackingSolver.Direction.Down:
				return "v";
			case BackTrackingSolver.Direction.Left:
				return "<";
			case BackTrackingSolver.Direction.Right:
				return ">";
		}
	}

	export class State implements BackTrackingSolver.IState {

		colors: { [coord: string]: ICell }

		constructor(public width: number, public height: number) {
			this.colors = {};

			for (var y = 0; y < height; y++) {
				for (var x = 0; x < width; x++) {
					this.colors[x + "x" + y] = {
						color: Colors.Gray,
						dots: 0
					};
				}
			}
		}

		setCells(cells: [{coord: string, cell: ICell}]) {
			var n = cells.length;
			for (var i = 0; i < n; i++) {
				this.colors[cells[i].coord] = cells[i].cell;
			}
		}

		toString(): string {
			var result: string = "";
			for (var y = 0; y < this.height; y++) {
				for (var x = 0; x < this.width; x++) {
					var cell = this.colors[x + "x" + y];
					result += color2char(cell.color) + (cell.dots > 0 ? "(" + cell.dots + ") " : "    ");
				}
				result += "\n";
			}
			return result;
		}

		clone(): State {
			var result = new State(this.width, this.height);

			for (var y = 0; y < this.height; y++) {
				for (var x = 0; x < this.width; x++) {
					var key = x + "x" + y;
					var cell = this.colors[key];
					result.colors[key] = { color: cell.color, dots: cell.dots };
				}
			}
			return result;
		}
	}

	export class Game implements BackTrackingSolver.IGame {

		constructor(public targetColor: Colors) {}

		isAcceptState(state: State): boolean {
			for (var y = 0; y < state.height; y++) {
				for (var x = 0; x < state.width; x++) {
					var key = x + "x" + y;
					var cell = state.colors[key];
					if (cell.color != Colors.Gray && cell.color != this.targetColor) return false;
				}
			}
			return true;
		}

		isRejectState(state: State): boolean {

			var allZeroDots: boolean = false;
			var someNonTargetColor: boolean = false;
			var zeroCounter: number = 0;
			var allCounter: number = 0;

			for (var y = 0; y < state.height; y++) {
				for (var x = 0; x < state.width; x++) {
					var key = x + "x" + y;
					var cell = state.colors[key];
					if (cell.color == Colors.Gray) continue;

					allCounter++;
					if (cell.dots == 0) zeroCounter++;
					if (cell.color != this.targetColor) someNonTargetColor = true;
				}
			}
			return (zeroCounter == allCounter) && someNonTargetColor;
		}

		private floodFill(state: State, coord: BackTrackingSolver.Coord, startColor: Colors, targetColor: Colors) {
			if (targetColor == startColor) return;

			var cell = state.colors[coord.toString()];
			if (cell.color != startColor) return;

			cell.color = targetColor;

			var neighbors = coord.neighbors(state.width, state.height);
			for (var i = 0; i < neighbors.length; i++) {
				this.floodFill(state, neighbors[i], startColor, targetColor);
			}

		}

		apply(state: State, move: Move): State {
			var newState = state.clone();

			var oldCoord = move.point.toString();
			var oldCell = state.colors[oldCoord];
			var targetCoord = move.point.neighbor(move.direction);
			var targetCoordS = targetCoord.toString();
			var targetCell = newState.colors[targetCoord.toString()];

			if (state.colors[targetCoordS].color != Colors.Gray) {
				// flood fill
				newState.colors[oldCoord].dots--;
				this.floodFill(newState, targetCoord, targetCell.color, oldCell.color);

			} else {
				// move
				newState.colors[targetCoordS] = {
					color: oldCell.color,
					dots: oldCell.dots - 1
				};
				newState.colors[oldCoord] = {
					color: Colors.Gray,
					dots: 0
				}
			}

			return newState;
		}

		getPlausibleMoves(state: State): Move[] {
			var result = [];

			for (var y = 0; y < state.height; y++) {
				for (var x = 0; x < state.width; x++) {

					var key1 = x + "x" + y;
					var cell = state.colors[key1];
					if (cell.dots == 0) continue;

					// up?
					if (y > 0) {
						var upCell = state.colors[x + "x" + (y - 1)];
						if (upCell.color != cell.color) {
							result.push(new Move(new BackTrackingSolver.Coord(x, y), BackTrackingSolver.Direction.Up));
						} 
					}

					// down?
					if (y < (state.height - 1)) {
						var downCell = state.colors[x + "x" + (y + 1)];
						if (downCell.color != cell.color) {
							result.push(new Move(new BackTrackingSolver.Coord(x, y), BackTrackingSolver.Direction.Down));
						}
					}

					// left?
					if (x > 0) {
						var leftCell = state.colors[(x - 1) + "x" + y];
						if (leftCell.color != cell.color) {
							result.push(new Move(new BackTrackingSolver.Coord(x, y), BackTrackingSolver.Direction.Left));
						}
					}

					// right? 
					if (x < (state.width - 1)) {
						var rightCell = state.colors[(x + 1) + "x" + y];
						if (rightCell.color != cell.color) {
							result.push(new Move(new BackTrackingSolver.Coord(x, y), BackTrackingSolver.Direction.Right));
						}
					}
				}
			}

			return result;
		}
	}
}
 