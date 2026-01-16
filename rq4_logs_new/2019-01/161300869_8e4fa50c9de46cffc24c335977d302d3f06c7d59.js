// Nicky Devilee | 99047338
// Bol4 applicatieontwikkelaar
// Blok 2
// Week 11-14 - Lingo

var hetWoord;
var aantalInvoers;
var geheimeLetters;
var inputwoord;


function createElements() {
	var container = document.getElementById("container");
	var teller = 1;
	for (var f = 0; f < 5 ; f++) {
		
	
	var div = document.createElement("div");

	container.appendChild(div);
	div.id = teller;
	for (var i = 1; i < 6; i++) {
	var input = document.createElement("input");
	div.appendChild(input);
	input.className = teller;
	input.setAttribute("maxlength", "1");
	}
	teller++;
	}

	var checkButton = document.createElement("button");
	container.appendChild(checkButton);
	checkButton.innerHTML = "Check woord?";
	checkButton.id = "checkInput";
	var check = document.getElementById('checkInput');
	check.onclick = checkInput;

	var opnieuwButton = document.createElement("button");
	container.appendChild(opnieuwButton);
	opnieuwButton.innerHTML = "Start opnieuw!";
	opnieuwButton.id = "opnieuw";
	var opnieuw = document.getElementById('opnieuw');
	opnieuw.onclick = resetGame;
}

function firstLetter(word) {

	aantalInvoers = 1;
	hetWoord = words[Math.floor(Math.random() * words.length)];

	for (var i = 1; i < 6 ; i++) {
	this.word = hetWoord;
	var woord = document.getElementsByClassName(aantalInvoers)
	woord[0].value = hetWoord[0]; // laat de eerste letter zien van het woord en zet het in de 1ste input.
	geheimeLetters = hetWoord.split(" ");
	console.log(hetWoord);
	aantalInvoers++;
	}

	aantalInvoers = 1;
}

function checkInput() {
	var woordCheck = [];
	var lettersLeft = [];


	var woord = document.getElementsByClassName(aantalInvoers);
	inputWoord = '';

	for (var i = 0; i < hetWoord.length; i++) {
		var letter = woord[i].value;
		inputWoord += letter;
		woordCheck[i] = false;
		lettersLeft[i] = letter;


		woord[i].style.backgroundColor = "red";

		if (letter === hetWoord[i]) {
			woord[i].style.backgroundColor = "green";
			var volgendeRij = aantalInvoers+1;
			if (volgendeRij <= 5) {
			document.getElementsByClassName(volgendeRij)[i].value = letter;
			}

			woordCheck[i] = true;
			lettersLeft[i] = false

		}
	}

	console.log(lettersLeft);
	console.log(woordCheck);

	if (inputWoord === hetWoord) {
		alert("Je hebt gewonnen! Het juiste woord was inderdaad " + hetWoord);
		resetGame();
	}

	else{	

		for (var i = 0; i < lettersLeft.length; i++) {
			if (lettersLeft[i] != false) {
				for (var s = 0; s < woordCheck.length; s++) {
					if (woordCheck[s] === false) {
						if(hetWoord[s] === lettersLeft[i]) {
							woord[i].style.backgroundColor = "yellow";
							lettersLeft[i] = false;
							woordCheck[s] = true;
						}
					}
				}
			}
		}

		aantalInvoers++;
		woord[0].value = hetWoord[0];

	}	
}

function resetGame() {
	location.reload();

}


createElements();
firstLetter();