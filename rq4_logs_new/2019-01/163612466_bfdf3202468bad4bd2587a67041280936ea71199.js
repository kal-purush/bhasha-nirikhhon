class Board {
	constructor(mapSize) {
		// sets standard map size to 4 x 4 or whatever is setted up in a constant
		if (mapSize == undefined) mapSize = MAP_SIZE;

		this.mapSize = mapSize;
		this.score = 0;
		this.emptyCells = new List(this.mapSize);

		// initializes map with zeros
		this.map = [];
		for (let i = 0; i < mapSize; i++) {
			this.map[i] = [];
			for (let j = 0; j < mapSize; j++)
				this.map[i][j] = 0;
		}
	}

	// checks if the map is full
	// returns true if there are no empty cells and false otherwise
	isFull() {
		return (this.emptyCells.size() == 0);
	}

	// for square two-dimensinal arrays
	arraysEqual(a1, a2) {
		let equal = true;
		if (a1.length != a2.length) return false;
		for (let i = 0; i < a1.length; i++) {
			for (let j = 0; j < a1.length; j++) {
				if (a1[i][j] != a2[i][j]) return false;
			}
		}
		return true;
	}

	// checks whether something will change if user will make a move
	isMovePossible() {
		// if map is not full we always can make a move
		if(!this.isFull()) return true;

		// map is full so we can make a move only if there are two adjacent tiles with the same value
		// let's check only down and right sides to avoid repetitions
		for (let i = 0; i < this.mapSize; i++) {
			for (let j = 0; j < this.mapSize; j++) {
				if (i != this.mapSize - 1 && this.map[i][j] == this.map[i + 1][j]) {
					// we can move in a vertical direction
					return true;
				}
				if (j != this.mapSize - 1 && this.map[i][j] == this.map[i][j + 1]) {
					// we can move in a horizontal direction
					return true;
				}
			}
		}

		// move is impossible
		return false;
	}

	// spawns new 2 or 4 tile to the map or does nothing if the map is full
	spawnNew() {
		if (this.isFull()) return;
		// gets random empty cell and spawns there a tile
		let k = Math.floor(Math.random() * this.emptyCells.size());
		let i = this.emptyCells.getK(k).i;
		let j = this.emptyCells.getK(k).j;
		if (Math.random() < 0.1) {
			this.map[i][j] = 4;
		}
		else {	
			this.map[i][j] = 2;
		}
		this.emptyCells.delete(i, j);
	}

	// makes a move in a specified direction and returns true if the map was changed and false otherwise
	moveMap(direction) {
		// flag that is true if at least one tile has changed it's location
		let somethingChanged = false;

		/*
		 * array that stores information about tiles that was changed 
		 * we need it because by the rules we can't change the same tile twice so that 2222 
		 * can't be transformed to 8, we can transform it only to 44 
		 */
		let changedTiles = [];
		for (let i = 0; i < this.mapSize; i++) {
			changedTiles[i] = [];
			for (let j = 0; j < this.mapSize; j++) {
				changedTiles[i][j] = false;
			}
		}


		/*
		 * loop 1 - loop through columns or rows
		 * loop 2 - nested loop through tiles in a current column or row
		 * loop 3 - third loop which is when the current tile is already choosed and we 
		 *	 		have to watch the current column / row through to find the deadlock
		 */

		// true when we have to exit from this loop
		let loop1Exit = false;
		let loop2Exit = false;

		// loops variables
		let i = 0;
		let j = 0;
		let k = 0;

		// needs for every iteration through first loop, iterates first loop variable
		let loop1 = function(direction) {
			switch(direction) {
				case Direction.UP:
				case Direction.DOWN:
				// j from 0 to mapSize
				if (j < this.mapSize - 1) {
					j++;
				} else {
					loop1Exit = true;
				}
				break;

				case Direction.LEFT:
				case Direction.RIGHT:
				// i from 0 to mapSize
				if (i < this.mapSize - 1) {
					i++;
				} else {
					loop1Exit = true;
				}
				break;
			}
		}.bind(this);

		// needs for loop 2 variables initialization
		let loop2Init = function(direction) {
			switch(direction) {
				case Direction.UP:
				i = 1;
				break;

				case Direction.DOWN:
				i = this.mapSize - 2;
				break;

				case Direction.LEFT:
				j = 1;
				break;

				case Direction.RIGHT:
				j = this.mapSize - 2;
				break;
			}
		}.bind(this);

		// needs for every iteration through first loop, iterates second loop variable
		let loop2 = function(direction) {
			switch(direction) {
				case Direction.UP:
				// i from 1 to mapSize 
				if (i < this.mapSize - 1) {
					i++;
				} else {
					loop2Exit = true;
				}
				break;

				case Direction.DOWN:
				// i from mapSize - 2 to 0 inclusive
				if (i > 0) {
					i--;
				} else {
					loop2Exit = true;
				}
				break;

				case Direction.LEFT:
				// j from 1 to mapSize
				if (j < this.mapSize - 1) {
					j++;
				} else {
					loop2Exit = true;
				}
				break; 

				case Direction.RIGHT:
				// j from mapSize - 2 to 0 inclusive
				if (j > 0) {
					j--;
				} else {
					loop2Exit = true;
				}
				break;
			}
		}.bind(this);

		// needs for loop 2 variable initialization
		let loop3Init = function(direction) {
			switch(direction) {
				case Direction.UP: 
				case Direction.DOWN:
				k = i;
				break;

				case Direction.LEFT: 
				case Direction.RIGHT:
				k = j;
				break;
			}
		}.bind(this);

		// needs for every iteration through third loop, iterates k and
		// returns two values which are indexes for current tile looking for 
		let loop3 = function(direction) {
			switch(direction) {
				case Direction.UP:
				k--;
				return new Cell(k, j);

				case Direction.DOWN:
				k++;
				return new Cell(k, j);
				
				case Direction.LEFT:
				k--;
				return new Cell(i, k);
				
				case Direction.RIGHT:
				k++;
				return new Cell(i, k);
			}
		}

		// main loop
		while (!loop1Exit) {
			
			loop2Init(direction);
			while (!loop2Exit) {
				// loop code

				// empty tile, can't be transfered
				if (this.map[i][j] == 0) {					
					loop2(direction);
					continue;
				} 


				// current tile that may be transfered
				let currentValue = this.map[i][j];

				// tile where we are going to place the current one
				let newTile = new Cell(i, j);
				loop3Init(direction);
				while (true) {
					// tile that we are currently looking at in the loop represented as map coordinates
					let loopTile = loop3(direction);

					// out of range
					if (k < 0 || k >= this.mapSize) break;

					// tile that we are currently looking at value
					let loopTileValue = this.map[loopTile.i][loopTile.j];


					// empty loop, current can be transfered here
					if (loopTileValue == 0) {
						newTile = loopTile;
						continue;
					}

					// different tile, search stops
					if (loopTileValue != currentValue) {
						break;
					}

					// the same tile, we should transfer the current one there so the search stops
					if (loopTileValue == currentValue && changedTiles[loopTile.i][loopTile.j] == false) {
						newTile = loopTile;
						break;
					}

					// the same tile but it was already changed so we can't use it and have to use the previous one 
					if (loopTileValue == currentValue && changedTiles[loopTile.i][loopTile.j] == true) {
						break;
					}
				}

				// we are in deadlock, can't transfer current tile
				if (newTile.i == i && newTile.j == j) {
					loop2(direction);
					continue;
				}

				// erasing current value in map 
				this.map[i][j] = 0;
				this.emptyCells.addNew(i, j);
				somethingChanged = true;

				let newTileValue =this.map[newTile.i][newTile.j];

				// simply inserting current tile if insertion place is empty
				if (newTileValue == 0) {
					this.map[newTile.i][newTile.j] = currentValue;
					this.emptyCells.delete(newTile.i, newTile.j);
					
					loop2(direction);
					continue;
				}

				// doubling new tile value, increasing score, marking new tile as changed
				if (newTileValue == currentValue) {
					this.map[newTile.i][newTile.j] = currentValue * 2;
					changedTiles[newTile.i][newTile.j] = true;
					this.score += currentValue * 2;

					loop2(direction);
					continue;
				}

				loop2(direction);
			}
			loop2Exit = false;

			loop1(direction);
		}

		return somethingChanged;
	}

	// makes a move in a specified direction and spawns new tile if it's possible
	move(direction) {
		let somethingChanged = this.moveMap(direction);
		if (somethingChanged) {
			this.spawnNew();
		}
	}
}