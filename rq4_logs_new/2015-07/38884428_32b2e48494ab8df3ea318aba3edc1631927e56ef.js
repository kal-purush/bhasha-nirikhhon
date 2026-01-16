var $body,
		$display,
		$board,
		$tiles,
		$randomNumber,
		combination = [],
		sequence = [],
		guess = [],
		base = 2,
		level = Math.pow(base,2),
		boardWidth = boardHeight = 600,
		tileWidth,
		tileHeight,

compareArrays = function() {
	// Turn them into a string and compare
	// ... you can't compare an [1,2] with [1,2]
	return sequence.toString() === guess.toString();
}

checkWin = function(){
	if (compareArrays()) {
		$display.text("Correct");
		// Increase the base by 1
		base = base+1;
		level = Math.pow(base,2);
	} else {
		$display.text("Try again");
	};

	combination = [];
	sequence = [];
	guess = [];

	// Remove the board's active state
	$board.removeClass("active");

	// Remove the mousedown event listeners
	$tiles.off();

	setupBoard(createCombination)
}

playGame = function() {
	// Remove the board's active state for li hover
	$board.addClass("active");

	$tiles.on("click", function(){
		guess.push($tiles.index(this));
		if (guess.length === base) {
			// Run the code to check for a win
			checkWin();
		}
	});

	// Setup the mousedown and up states...
	$tiles.on("mousedown", function(){
		$(this).addClass("mousedown");
	})
	$tiles.on("mouseup", function(){
		$(this).removeClass("mousedown");
	})
},

chooseRandomTile = function(i){
	$tiles = $('li');
	randomNumber = Math.round(Math.random() * ($tiles.length -1));
	
	// Replace in combination array, the random number
	combination[i] = randomNumber;

	// Return the randomly selected tile
	return $tiles[randomNumber]
},

setupTiles = function(callback) {
	var i = 0;

	// Loop for the level number
	for (i; i<level; i++) {

		// Create a new li in 'memory'
		var tile = document.createElement('li'),

				// Make into a jQuery object 
				$tile = $(tile);

		$tile.addClass('tile');
		$tile.css('width', tileWidth+"px");
		$tile.css('height', tileHeight+"px");
		$board.append($tile);
	}

	// Run the function we passed in as an argument
	// ...in this case, createCombination
	callback();
},

setupBoard = function(callback) {
	// Calculate width of tiles using level
	tileWidth = tileHeight = boardWidth/Math.sqrt(level);

	// Create a new element in 'memory' using pure JS
	var board = document.createElement('ul'),
			display = document.createElement('h1');

	// On the left-hand side the $ is just part of the variable name 
	$body = $('body');
	$display = $(display);

	// Empty the body to make sure
	$body.empty();

	// Make the element in 'memory' a jQuery Object
	$board = $(board);

	// To make the ul -> <ul class="board"></ul> 
	// ul is still in the memory
	$board.addClass('board');

	// Add a width and height to the board
	// ul is still in memory
	$board.css('width', boardWidth+"px");
	$board.css('height', boardHeight+"px");

	$display.html("Let's start...")

	// Add display & board to body
	$body.append($display).append($board);

	// Callback is still the createCombination function
	setupTiles(callback);
},

createCombination = function() {
	var i = 0;

	// Loop for the base number
	// ... Into an empty array saved in combination
	// e.g. for 2, ---> [0,1]
	for (i; i<base; i++) {
		combination.push(i)
	}

	// For each one of those...
	combination.forEach(function(i){

		// Run this:
		setTimeout(function(){

			// Choose a random tile
			// ... pass i
			var tile = chooseRandomTile(i),
					$tile = $(tile);

			$tile.addClass('on');
			
			setTimeout(function(){
				$tile.removeClass('on');

				// Add all sequence indexes to an array
				sequence.push($tiles.index(tile));

				// If there are the same number of elements in the 
				// sequence array as the number of the base
				// All tiles have been shown.
				if (sequence.length === base) {
					playGame();
				}

			}, 500);

		}, (i+1)*1000);
	});
}

start = function(){
	setupBoard(createCombination)
};

// Document ready
// similar to window.onload
// Another way of writing:
// ... $(document).ready(){}
$(function(){
	start();
});