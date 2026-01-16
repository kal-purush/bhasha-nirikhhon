var game = new Array(4);
var adjusted = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0];
var score = 0;

$(document).ready(function(){
	$("#game").append("<table id='gametable'>");
	for(var i = 0; i < 4; i++){
		$("#game table").append("<tr class='gamerow'>");
		for(var j = 0; j < 4; j++){
			$("#game tr").last().append("<td class='gamecell' id='" + i + "-" + j + "'>")
		}
	}
	beginGame();
	updateColors(-1);
	$("#cover").fadeOut(6500);
	$("#covertext").delay(750).animate({
		fontSize: '32px',
		top: '-=252px',
		left: '-=47px'
	},3500);
	$("#gametitle").delay(4500).animate({
		opacity: '1',
	},1000);
	$("#scorediv").delay(5000).animate({
		opacity: '1',
	},500);
	$("#scorediv").effect("bounce", {times: 3}, "slow");
	gameloop();
});

function beginGame(){
	var rand1, rand2;
	while(rand1 == rand2){
		rand1 = Math.floor(Math.random() * 12);
		rand2 = Math.floor(Math.random() * 12);
	}
	var row1 = Math.floor(rand1/4);
	var col1 = rand1%4;
	var row2 = Math.floor(rand2/4);
	var col2 = rand2%4;
	$("#"+row1+"-"+col1).html("2");
	$("#"+row2+"-"+col2).html("2");
	for(var i = 0; i < 4; i++){
		var innerarray = new Array(4);
		game[i] = innerarray;
		for(var j = 0; j < 4; j++){
			game[i][j] = 0;
		}
	}
	game[row1][col1] = 2;
	game[row2][col2] = 2;
}

function updateColors(newindex){
	for(var i = 0; i < 4; i++){
		for(var j = 0; j < 4; j++){
			if(4*i+j != newindex){
				switch(game[i][j]){
					case 2048:
						$("#"+i+"-"+j).html("2048");
						$("#"+i+"-"+j).css("background-color", "DeepSkyBlue");
						break;
					case 1024:
						$("#"+i+"-"+j).html("1024");
						$("#"+i+"-"+j).css("background-color", "Aquamarine");
						break;
					case 512:
						$("#"+i+"-"+j).html("512");
						$("#"+i+"-"+j).css("background-color", "PaleGreen");
						break;
					case 256:
						$("#"+i+"-"+j).html("256");
						$("#"+i+"-"+j).css("background-color", "Gold");
						break;
					case 128:
						$("#"+i+"-"+j).html("128");
						$("#"+i+"-"+j).css("background-color", "LemonChiffon");
						break;
					case 64:
						$("#"+i+"-"+j).html("64");
						$("#"+i+"-"+j).css("background-color", "Salmon");
						break;
					case 32:
						$("#"+i+"-"+j).html("32");
						$("#"+i+"-"+j).css("background-color", "OrangeRed");
						break;
					case 16:
						$("#"+i+"-"+j).html("16");
						$("#"+i+"-"+j).css("background-color", "Tomato");
						break;
					case 8:
						$("#"+i+"-"+j).html("8");
						$("#"+i+"-"+j).css("background-color", "SandyBrown");
						break;
					case 4:
						$("#"+i+"-"+j).html("4");
						$("#"+i+"-"+j).css("background-color", "Tan");
						break;
					case 2:
						$("#"+i+"-"+j).html("2");
						$("#"+i+"-"+j).css("background-color", "LightGrey");
						break;
					default:
						$("#"+i+"-"+j).html("");
						$("#"+i+"-"+j).css("background-color", "White");
						break;
				}
			}
		}
	}
	if(newindex != -1){
		var r = Math.floor(newindex/4);
		var c = newindex%4;
		switch(game[r][c]){
			case 4:
				$("#"+r+"-"+c).html("4");
				$("#"+r+"-"+c).animate({
					backgroundColor: "#D2B48C"
				},400);
				break;
			case 2:
				$("#"+r+"-"+c).html("2");
				$("#"+r+"-"+c).animate({
					backgroundColor: "#D3D3D3"
				},400);
				break;
		}
	}
}

function gameloop(){
	$(document).on('keyup', function(e){
		if(e.keyCode == 37){
			moveLeft();
		}else if(e.keyCode == 38){
			moveUp();
		}else if(e.keyCode == 39){
			moveRight();
		}else if(e.keyCode == 40){
			moveDown();
		}
		adjusted = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0];
	});
}

function moveLeft(){
	var didmove = false;
	for(var i = 0; i < 4; i++){
		for(var j = 1; j < 4; j++){
			if(game[i][j] != 0){
				var k = 1;
				while(game[i][j - k] == 0 && (j-k) >= 0){
					k++;
				}
				if(j-k >= 0){
					if(checkCollision(game[i][j], i, j, i, j-k))
						didmove = true;
				}
				if(k > 1){
					game[i][j-k+1] = game[i][j];
					game[i][j] = 0;
					didmove = true;
				}
			}
		}
	}
	updateColors(-1);
	addBlock(didmove);
}

function moveRight(){
	var didmove = false;
	for(var i = 0; i < 4; i++){
		for(var j = 2; j >= 0; j--){
			if(game[i][j] != 0){
				var k = 1;
				while(game[i][j + k] == 0 && (j+k) <= 3){
					k++;
				}
				if(j+k < 4){
					if(checkCollision(game[i][j], i, j, i, j+k))
						didmove = true;
				}
				if(k > 1){
					game[i][j+k-1] = game[i][j];
					game[i][j] = 0;
					didmove = true;
				}
			}
		}
	}
	updateColors(-1);
	addBlock(didmove);
}

function moveUp(){
	var didmove = false;
	for(var i = 0; i < 4; i++){
		for(var j = 1; j < 4; j++){
			if(game[j][i] != 0){
				var k = 1;
				while((j-k) >= 0 && game[j - k][i] == 0){
					k++;
				}
				if(j-k >= 0){
					if(checkCollision(game[j][i], j, i, j-k, i))
						didmove = true;
				}
				if(k > 1){
					game[j-k+1][i] = game[j][i];
					game[j][i] = 0;
					didmove = true;
				}
			}
		}
	}
	updateColors(-1);
	addBlock(didmove);
}

function moveDown(){
	var didmove = false;
	for(var i = 0; i < 4; i++){
		for(var j = 2; j >= 0; j--){
			if(game[j][i] != 0){
				var k = 1;
				while((j+k) <= 3 && game[j+k][i] == 0){
					k++;
				}
				if(j+k < 4){
					if(checkCollision(game[j][i], j, i, j+k, i))
						didmove = true;
				}
				if(k > 1){
					game[j+k-1][i] = game[j][i];
					game[j][i] = 0;
					didmove = true;
				}
			}
		}
	}
	updateColors(-1);
	addBlock(didmove);
}

function checkCollision(num, orow, ocol, row, col){
	if(adjusted[row*4+col] == 0){
		if(num == game[row][col]){
			game[orow][ocol] = 0;
			game[row][col] = 2 * num;
			score += 2 * num;
			$("#score").html(score);
			checkHighScore(score);
			adjusted[row*4+col] = 1;
			return true;
		}else{
			return false;
		}
	}else{
		return false;
	}
	
}

function addBlock(didmove){
	open = [];
	for(var i = 0; i < 4; i++){
		for(var j = 0; j < 4; j++){
			if(game[i][j] == 0){
				open.push((i*4)+j);
			}
		}
	}
	if(open.length == 0){
		recordHighScore(score);

	}else{
		if(didmove){
			var rand = Math.floor(Math.random() * open.length);
			var r = Math.floor(open[rand]/4);
			var c = open[rand]%4;
			var rand1 = Math.floor(Math.random() * 10);
			if(rand1 == 9){
				game[r][c] = 4;
			}else{
				game[r][c] = 2;
			}
			updateColors(r*4+c);
		}
	}
}