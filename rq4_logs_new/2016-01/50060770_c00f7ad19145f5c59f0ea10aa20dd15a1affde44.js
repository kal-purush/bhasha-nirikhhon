$(document).ready(function() {
	var move = 0;
	var play = true;
	var nextPlayer;
	var winningOne;
	var winningTwo;
	var winningThree;

	$(".square").on("click", function() {
		if ($(this).text() === "" && play) {
			if (move%2 === 0) {
				nextPlayer = "O";
				$(this).append("X");
				$("#message").text(nextPlayer + "\'s Turn!");
			} else {
				nextPlayer = "X";
				$(this).append("O");
				$("#message").text(nextPlayer + "\'s Turn!")
			}
			move++;
			if (checkForWinner() !== -1 && checkForWinner() !== "") {
				if (checkForWinner() === "X") {
					$("#message").text("X Wins!");
					winningOne.css("background", "#FF6965");
					winningTwo.css("background", "#FF6965");
					winningThree.css("background", "#FF6965");
				} else {
					$("#message").text("O Wins!");
					winningOne.css("background", "#FF6965");
					winningTwo.css("background", "#FF6965");
					winningThree.css("background", "#FF6965");
				}
				play = false;
			}
			if (move === 9 && checkForWinner() === -1) {
				$("#message").text("Cat's Game!");
			}
		}
	});

	$("#newGame").on("click", function() {
		play = true;
		move = 0;
		$(".square").text("");
		$(".square").css("background", "none");
		$("#message").text("X's Turn!");
	});

	function checkForWinner() {
		var one = $("#1").text();
		var two = $("#2").text();
		var three = $("#3").text();
		var four = $("#4").text();
		var five = $("#5").text();
		var six = $("#6").text();
		var seven = $("#7").text();
		var eight = $("#8").text();
		var nine = $("#9").text();

		// check rows
		if ((one === two) && (two === three)) {
			winningOne = $("#1");
			winningTwo = $("#2");
			winningThree = $("#3");
			return three;
		} else if ((four === five) && (five === six)) {
			winningOne = $("#4");
			winningTwo = $("#5");
			winningThree = $("#6");
			return six;
		} else if ((seven === eight) && (eight === nine)) {
			winningOne = $("#7");
			winningTwo = $("#8");
			winningThree = $("#9");
			return nine;
		}
		// check columns
		else if ((one === four) && (four === seven)) {
			winningOne = $("#1");
			winningTwo = $("#4");
			winningThree = $("#7");
			return seven;
		} else if ((two === five) && (five === eight)) {
			winningOne = $("#2");
			winningTwo = $("#5");
			winningThree = $("#8");
			return eight;
		} else if ((three === six) && (six === nine)) {
			winningOne = $("#3");
			winningTwo = $("#6");
			winningThree = $("#9");
			return nine;
		}
		// check diagnals
		else if ((one === five) && (five === nine)) {
			winningOne = $("#1");
			winningTwo = $("#5");
			winningThree = $("#9");
			return nine;
		} else if ((three === five) && (five === seven)) {
			winningOne = $("#3");
			winningTwo = $("#5");
			winningThree = $("#7");
			return seven;
		}
		// no winner
		return -1;
	};

});