let player = "X";
		let squares = document.querySelectorAll(".square");
		let resultModal = document.getElementById("result-modal");
		let happy = document.getElementById("happy");
		let sad = document.getElementById("sad");
		let resultText = document.getElementById("result-text");
		let container = document.getElementById("container");

		function handleClick(e) {
			let square = e.target;
			if (square.innerHTML === "") {
				square.innerHTML = player;
				checkForWinner();
				player = player === "X" ? "O" : "X";
			}
		}

		function checkForWinner() {
			let winningCombos = [
				[0, 1, 2],
				[3, 4, 5],
				[6, 7, 8],
				[0, 3, 6],
				[1, 4, 7],
				[2, 5, 8],
				[0, 4, 8],
				[2, 4, 6]
			];

			for (let combo of winningCombos) {
				let [a, b, c] = combo;
				if (squares[a].innerHTML !== "" && squares[a].innerHTML === squares[b].innerHTML && squares[b].innerHTML === squares[c].innerHTML) {
                    happy.style.display = "block";
					resultText.innerHTML = ` Congratulations player  = "${squares[a].innerHTML}" win the match!`;
					resultModal.style.display = "block";
					resetBoard();
					return;
				}
			}

			let draw = true;
			for (let square of squares) {
				if (square.innerHTML === "") {
					draw = false;
					break;
				}
			}

			if (draw) {
                sad.style.display = "block";
				resultText.innerHTML = "So sad the match is a draw! try again";
				resultModal.style.display = "block";
				resetBoard();
			}
		}

		function resetBoard() {
			for (let square of squares) {
				square.innerHTML = "";
			}
		}

		for (let square of squares) {
			square.addEventListener("click", handleClick);
		}

		let closeModal = document.getElementsByClassName("close")[0];
		closeModal.onclick = function() {
			resultModal.style.display = "none";
            happy.style.display = "none";
            sad.style.display = "none";

		}
		

		window.onclick = function(event) {
			if (event.target == resultModal) {
				resultModal.style.display = "none";
			}
		}