function $(id) {
	return document.getElementById(id);
}

var canvas;
var cells;
var cellSize;
var colors = ["Black", "white"];
var ctx;
var gridSize;
var updateSpeed;
var generation;
var running;

window.onload = function() {
	canvas = $("canvas");
	// set context of the canvas
	ctx = canvas.getContext("2d");
	
	// Listeners for input and options
	$("new").addEventListener("click",newGame);
	$("start").addEventListener("click",start);
	$("stop").addEventListener("click", stop);
	$("step").addEventListener("click", step);
	
	// Canvas click listener
	$("canvas").addEventListener("click", function(e) { canvasClick(e)});
	
	// Load a new game
	newGame();
}

// initialize a new array of cells of random state
function initializeCells() {
	console.log("Initializing cells...");
	cells = [];
	for (var i = 0; i < gridSize; i++) {
		cells[i] = [];
		for (var j = 0; j < gridSize; j++) {
			cells[i][j] = {};
			cells[i][j].x = i; // grid x position
			cells[i][j].y = j; // grid y position
			// set random value
			cells[i][j].current = (Math.random() < 0.5) ? true : false;
			cells[i][j].future = false; // is alive in next generation
			cells[i][j].age = (cells[i][j].current) ? 1 : 0; // age(generations)
			cells[i][j].color = colors[cells[i][j].age]; // cell color
			// console.log("Cell [" + cells[i][j].x + "[]" + cells[i][j].y + "] is " + cells[i][j].age);
		}
	}
	console.log("Done initializing cells!");
}

// check and return the number of live neighbors a cell has
function checkNeighbors(cell) {
	var liveNeighbors = 0;
	var x = cell.x;
	var y = cell.y;
	var xOffset;
	var yOffset;
	
	// check how many neighbors are alive
	for (var i = -1; i <= 1; i++) {
		for (var j = -1; j <= 1; j++) {
			if(i == 0 && j == 0) {
				continue;
			}
			if(x+i < 0) {
				// flow over to right side
				xOffset = gridSize - 1;
			}
			else if(x+i > gridSize -1) {
				// flow over to left side
				xOffset = -gridSize + 1;
			}
			else {
				xOffset = i;
			}
			
			if(y+j < 0) {
				// flow over to the bottom
				yOffset = gridSize - 1;
			}
			else if(y+j > gridSize -1) {
				// flow over to top
				yOffset = -gridSize + 1;
			}
			else {
				yOffset = j;
			}
			
			if(cells[x+xOffset][y+yOffset].current) {
				liveNeighbors++;
			}
		}
	}
	// return number of alive
	return liveNeighbors;
}

// move cells to next generation
function nextGen() {
	generation++;
	outputGeneration();
	for(var i = 0; i < gridSize; i++) {
		for(var j = 0; j < gridSize; j++) {
			// put future state as current and update
			cells[i][j].current = cells[i][j].future;
			if(cells[i][j].current) {
				cells[i][j].age++;
				cells[i][j].color = colors[1];
			}
			else  {
				cells[i][j].age = 0;
				cells[i][j].color = colors[0];
			}
		}
	}
}

// find future states
function future() {
	var neighbors;
	for (var i = 0; i < gridSize; i++) {
		for (var j = 0; j < gridSize; j++) {
			// update alive state for next generation
			// get number of live neighbors
			neighbors = checkNeighbors(cells[i][j]);
			// apply rules
			// if dead and have 3 neighbors become alive in future
			if (!cells[i][j].current && neighbors == 3) {
				// alive next generation
				cells[i][j].future = true;
			}
			// alive
			else if (cells[i][j].current) {
				// 2 or 3 neighbors
				if(neighbors == 2 || neighbors == 3) {
					// survive to next generation
					cells[i][j].future = true;
				}
				else {
					// dies
					cells[i][j].future = false;
				}
			}
		}
	}
	
}

// display cells
function displayCells() {
	for (var i = 0; i < gridSize; i++) {
		for (var j = 0; j < gridSize; j++) {
			// get pixel position of cell
			var loc = cellToCoord(cells[i][j].x, cells[i][j].y);
			// draw square of cell color
			ctx.beginPath();
			ctx.rect(loc.x,loc.y,cellSize,cellSize);
			ctx.stroke();
			ctx.fillStyle = cells[i][j].color;
			ctx.fill();
		}
	}
}

// start a new game
function newGame() {
	console.log("Starting new game...");
	running = false;
	generation = 0;
	setGridSize();		// set the value of gridSize
	// get size of cell
	setCellSize();
	// set update speed
	setUpdateSpeed();
	// initialize new array of random cells
	initializeCells();
	// display cells
	displayCells();
	// find future state
	future();
	// start game
	//running = true;
	setTimeout(update, updateSpeed);
}

// run update cycle
function update() {
	if(running) {
		// set current state to value of future
		nextGen();
		// find future states
		future();
		// draw updated data
		displayCells();
		// call next update
		setTimeout(update,updateSpeed);
	}
}

// start game
function start() {
	if(!running) {
		running = true;
		update();
	}
}

// stop game
function stop() {
	if(running) {
		running = false;
	}
}

// perform single generation
function step() {
	// set current state to value of future
	nextGen();
	// find future states
	future();
	// draw updated data
	displayCells();
}

// give info on clicked canvas position
function canvasClick(event) {
	var click = windowToCanvas(canvas, event.clientX, event.clientY);
	var loc = coordToCell(click.x, click.y);
	var x = loc.x, y = loc.y;
	$("cell").innerHTML = "Cell ("+x+","+y+")<br/>Current:"+cells[x][y].current+"<br/>Future:"+cells[x][y].future+"<br/>Age:"+cells[x][y].age+"<br/>Colors:"+cells[x][y].color+"<br/>Neighbors:"+checkNeighbors(cells[x][y]);
	//console.log("Cell ("+x+","+y+")\nCurrent:"+cells[x][y].current+"\nFuture:"+cells[x][y].future+"\nAge:"+cells[x][y].age+"\nColors:"+cells[x][y].color+"\nNeighbors:"+checkNeighbors(cells[x][y]));
}

// set the gridSize
function setGridSize() {
	if($("10cells").checked) {
		gridSize = $("10cells").value;
	}
	else if($("100cells").checked) {
		gridSize = $("100cells").value;
	}
	else if($("600cells").checked) {
		gridSize = $("600cells").value;
	}
	console.log("Grid size set to " + gridSize);
	// gridSize = getRadioVal("numCells");
	// console.log("Grid size set to " + gridSize);
}

// set the cellSize
function setCellSize() {
	cellSize = canvas.width / gridSize;
	console.log("Cell size set to " + cellSize);
}

// set update speed
function setUpdateSpeed() {
	if($("1sec").checked) {
		updateSpeed = $("1sec").value;
	}
	else if($("2sec").checked) {
		updateSpeed = $("2sec").value;
	}
	else if($("05sec").checked) {
		updateSpeed = $("05sec").value;
	}
	//updateSpeed = getRadioVal(updateSpeed);
	console.log("Update speed set to " + updateSpeed);
}

function outputGeneration() {
	$("generation").innerHTML = "Generation " + generation;
}

// utility scripts

// convert canvas pixel coord to cell position in cell grid
function coordToCell(x, y) {
	return { x: Math.floor(x / cellSize),
			 y: Math.floor(y / cellSize)	};
}

// convert cell position to canvas pixel coord
function cellToCoord(x, y) {
	// return x and y coord of the cell position in structure
	return { x: (x * cellSize),
			 y: (y * cellSize)};
}

// convert widow coords to canvas coords
function windowToCanvas(canvas, x, y) {
	var bbox = canvas.getBoundingClientRect();
	
	return { x: Math.floor(x - bbox.left * (canvas.width / bbox.width)),
			 y: Math.floor(y - bbox.top * (canvas.height / bbox.height))
	};
}

// get the value of a radio button group
function getRadioVal(name) {
    var val;
    // get list of radio buttons with specified name
    var radios = document.getElementsByName(name);
    
    // loop through list of radio buttons
    for (var i=0; i < radios.length; i++) {
        if ( radios[i].checked ) { // radio checked?
            val = radios[i].value; // if so, hold its value in val
            break; // and break out of for loop
        }
    }
    return val; // return value of checked radio or undefined if none checked
}