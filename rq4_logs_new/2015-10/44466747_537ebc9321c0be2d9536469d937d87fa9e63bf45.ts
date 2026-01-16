/// <reference path="../util.ts" />
/// <reference path="../interfaces.ts" />

module Hanoi {

	export class Move implements BackTrackingSolver.IMove {

		constructor(public from: number, public to: number) {}

		toString(): string {
			return this.from + " -> " + this.to;
		}
	}

	export class State implements BackTrackingSolver.IState {

		towers: {[towerIndex: number]: number[]}

		constructor(public height: number) {
			this.towers = {
				1: [],
				2: [],
				3: []
			}

			for (var i = height; i > 0; i--) {
				this.towers[1].push(i);
			}
		}		

		toString(): string {
			var result: string = "";
			for (var i = this.height; i >= 0; i--) {
				for (var tIndex = 1; tIndex <= 3; tIndex++) {
					var tower = this.towers[tIndex];
					result += (i < tower.length) ? (tower[i] + " ") : "| ";
				}
				result += "\n";
			}
			return result;
		}

		clone(): State {
			var result = new State(this.height);
			for (var tIndex = 1; tIndex <= 3; tIndex++) {
				result.towers[tIndex] = this.towers[tIndex].concat([]);
			}
			return result;
		}
	}

	export class Game implements BackTrackingSolver.IGame {

		isAcceptState(state: State): boolean {
			return state.towers[1].length == 0 && state.towers[2].length == 0;
		}

		isRejectState(state: State): boolean {
			return false;
		}

		apply(state: State, move: Move): State {
			var newState = state.clone();
			var item = newState.towers[move.from].pop();
			newState.towers[move.to].push(item);
			return newState;
		}

		getPlausibleMoves(state: State): Move[] {
			var result = [];

			for (var from = 1; from <= 3; from++) {
				for (var to = 1; to <= 3; to++) {
					if (from == to) continue;

					var fromTower = state.towers[from];
					var fromItem = fromTower[fromTower.length - 1];
					if (!fromItem) continue;

					var toTower = state.towers[to];
					var toItem = toTower[toTower.length - 1];

					if (!toItem || toItem > fromItem) {
						result.push(new Move(from, to));
					}
				}
			}

			return result;
		}
	}
}
 