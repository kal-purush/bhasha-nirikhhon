//Challenge 1
function ageInDays()
{
	var birthyear = prompt("What is your Birth Year?");
	var ageInDayss = (2020 - birthyear) * 365;
	console.log(ageInDayss);
	var h = document.createElement('h1');
	var textAnswer = document.createTextNode('You are ' + ageInDayss + ' Days Old');
	h.setAttribute('id','ageInDays');
	h.appendChild(textAnswer); 
	document.getElementById('flex-box-result').appendChild(h);
}
function reset()
{
	document.getElementById('ageInDays').remove();
}

//Challenge 2
function generateCat()
{
	var image = document.createElement('img');
	var div = document.getElementById('flex-cat-gen');
	image.src = "./images/cat.gif";
	div.appendChild(image);
}

//Challenge 3
function rpsGame(yourChoice)
{
	//console.log(yourChoice);
	//console.log(yourChoice.src);
	var humanChoice, botChoice;
	humanChoice = yourChoice.id;
	botChoice = numberToChoice(randToRpsInt());
	console.log('Computer Choice : ',botChoice);
	results = decideWinner(humanChoice, botChoice);
	console.log(results);
	//[0,1] Bot Won
	//[1,0] Human Won
	//[0.5,0.5] Draw
	message = finalMessage(results);
	console.log(message);
	rpsFrontend(humanChoice, botChoice, message);
}
function randToRpsInt()
{
	return Math.floor(Math.random()*3);
}
function numberToChoice(number)
{
	return ['rock','paper','scissors'][number];
}
function decideWinner(yourChoice, computerChoice)
{
	var rpsDatabase = {
		'rock': {'scissors':1, 'rock':0.5, 'paper':0},
		'paper': {'rock':1, 'paper':0.5, 'scissors':0},
		'scissors': {'rock':0, 'paper':1, 'scissors':0.5}
	}

	var yourScore = rpsDatabase[yourChoice][computerChoice];
	var computerScore = rpsDatabase[computerChoice][yourChoice];

	return [yourScore,computerScore];
}
function finalMessage([yourScore,computerScore])
{
	if(yourScore === 0)
	{
		return {'message': 'You Lost!', 'color':'red'};
	}
	else if(yourScore === 0.5)
	{
		return {'message': 'You Tied!', 'color':'yellow'};
	}
	else
	{
		return {'message': 'You Won!', 'color':'green'};
	}
}
function rpsFrontend(humanImageChoice, botImageChoice, finalMessage)
{
	var imagesDatabase = {
		'rock': document.getElementById('rock').src,
		'paper': document.getElementById('paper').src,
		'scissors': document.getElementById('scissors').src
	}

	document.getElementById('rock').remove();
	document.getElementById('paper').remove();
	document.getElementById('scissors').remove();

	var humanDiv = document.createElement('div');
	var messageDiv = document.createElement('div');
	var botDiv = document.createElement('div');

	humanDiv.innerHTML = "<img src='" + imagesDatabase[humanImageChoice] + "' style='box-shadow: 0px 10px 50px rgba(37, 50, 233, 1);'>";
	document.getElementById('flex-box-container-3').appendChild(humanDiv);

	messageDiv.innerHTML = "<h1 style='color: " + finalMessage['color'] + "; font-size: 60px; padding: 30px;'>" + finalMessage['message'] + "</h1>";
	document.getElementById('flex-box-container-3').appendChild(messageDiv);

	botDiv.innerHTML = "<img src='" + imagesDatabase[botImageChoice] + "' style='box-shadow: 0px 10px 50px rgba(243, 38, 24, 1);'>";
	document.getElementById('flex-box-container-3').appendChild(botDiv);
}

//Challenge 4
var all_buttons = document.getElementsByTagName('button');
//console.log(all_buttons);
var copyAllButtons = [];
for(let i=0;i<all_buttons.length;i++)
{
	copyAllButtons.push(all_buttons[i].classList[1]);
}
//console.log(copyAllButtons);
//console.log(copyAllButtons[0].classList);
//copyAllButtons[0].classList.remove('btn-primary');
//copyAllButtons[0].classList.add('btn-success');
function buttonColorChange(colorChoosed)
{
	//console.log(colorChoosed.value);
	if(colorChoosed.value === 'red')
	{
		buttonsRed();
	}
	else if(colorChoosed.value === 'green')
	{
		buttonsGreen();
	}
	else if(colorChoosed.value === 'reset')
	{
		buttonsReset();
	}
	else if(colorChoosed.value === 'random')
	{
		buttonsRandom();
	}
}
function buttonsRed()
{
	for(let i=0; i<all_buttons.length; i++)
	{
		all_buttons[i].classList.remove(all_buttons[i].classList[1]);
		all_buttons[i].classList.add('btn-danger');
	}
}
function buttonsGreen()
{
	for(let i=0; i<all_buttons.length; i++)
	{
		all_buttons[i].classList.remove(all_buttons[i].classList[1]);
		all_buttons[i].classList.add('btn-success');
	}
}
function buttonsReset()
{
	for(let i=0; i<all_buttons.length; i++)
	{
		all_buttons[i].classList.remove(all_buttons[i].classList[1]);
		all_buttons[i].classList.add(copyAllButtons[i]);
	}
}
function buttonsRandom()
{
	var choices = ['btn-primary', 'btn-danger', 'btn-warning', 'btn-success'];
	for(let i=0; i<all_buttons.length; i++)
	{
		var randNumber = Math.floor(Math.random() * 4);
		all_buttons[i].classList.remove(all_buttons[i].classList[1]);
		all_buttons[i].classList.add(choices[randNumber]);
	}
}

//Challenge 5
let blackjackGame = {
	'you': {'scoreSpan': '#your-blackjack-result', 'div': '#your-box', 'score': 0},
	'dealer': {'scoreSpan': '#dealer-blackjack-result', 'div': '#dealer-box', 'score': 0},
	'cards': ['2','3','4','5','6','7','8','9','10','K','Q','J','A'],
	'cardsMap': {'2': 2,'3': 3,'4': 4,'5': 5,'6': 6,'7': 7,'8': 8,'9': 9,'10': 10,'K': 10,'Q': 10,'J': 10,'A': [1,11]},
	'wins': 0,
	'losses': 0,
	'draws': 0,
	'isStand': false,
	'turnsOver': false
};


const YOU = blackjackGame['you'];
const DEALER = blackjackGame['dealer'];

//console.log(YOU);

const hitSound = new Audio('./sound/swish.m4a');
const winSound = new Audio('./sound/cash.mp3');
const lostSound = new Audio('./sound/aww.mp3');

document.getElementById('blackjack-hit-button').addEventListener("click", blackjackHit);
document.getElementById('blackjack-stand-button').addEventListener("click", dealerLogic);
document.getElementById('blackjack-deal-button').addEventListener("click", blackjackDeal);

function blackjackHit()
{
	if(blackjackGame['isStand'] === false)
	{
		let card = randomCards();
		showCard(card, YOU);
		updateScore(card, YOU);
		showScore(YOU);
	}
}
function randomCards()
{
	let randomIndex = Math.floor(Math.random() * 13);
	return blackjackGame['cards'][randomIndex];
}
function showCard(card, activePlayer)
{
	if(activePlayer['score'] <= 21)
	{
		let cardImage = document.createElement('img');
		cardImage.src = './images/' + card + '.png';
		document.querySelector(activePlayer['div']).appendChild(cardImage);
		hitSound.play();
	}
}
function blackjackDeal()
{
	if(blackjackGame['turnsOver'] === true)
	{
		blackjackGame['isStand'] = false;

		let yourImages = document.querySelector('#your-box').querySelectorAll('img');
		let dealerImages = document.querySelector('#dealer-box').querySelectorAll('img');
		//console.log(yourImages);
		for(let i=0; i<yourImages.length; i++)
		{
			yourImages[i].remove();
		}
		for(let i=0; i<dealerImages.length; i++)
		{
			dealerImages[i].remove();
		}
		YOU['score'] = 0;
		DEALER['score'] = 0;

		document.querySelector('#your-blackjack-result').style.color = "white";
		document.querySelector('#dealer-blackjack-result').style.color = "white";
		document.querySelector('#your-blackjack-result').textContent = 0;
		document.querySelector('#dealer-blackjack-result').textContent = 0;

		document.querySelector('#blackjack-result').textContent = "Let's Play";
		document.querySelector('#blackjack-result').style.color = "black";

		blackjackGame['turnsOver'] = false;
	}
	
}
function updateScore(card, activePlayer)
{
	//If adding 11 keeps me below 21 add 11 otherwise add 1
	if(card === 'A')
	{
		if(activePlayer['score'] + blackjackGame['cardsMap'][card][1] <=21)
		{
			activePlayer['score'] += blackjackGame['cardsMap'][card][1]; //add 11
		}
		else
		{
			activePlayer['score'] += blackjackGame['cardsMap'][card][0]; //add 1
		}
	}
	else
	{
		activePlayer['score'] += blackjackGame['cardsMap'][card]; 
	}
}
function showScore(activePlayer)
{
	if(activePlayer['score'] > 21)
	{
		document.querySelector(activePlayer['scoreSpan']).textContent = "BUST!";
		document.querySelector(activePlayer['scoreSpan']).style.color = "red";
	}
	else
	{
		document.querySelector(activePlayer['scoreSpan']).textContent = activePlayer['score'];
	}
}
function sleep(ms)
{
	return new Promise(resolve => setTimeout(resolve, ms));
}
async function dealerLogic()
{
	blackjackGame['isStand'] = true;

	while(DEALER['score'] < 16 && blackjackGame['isStand'] === true)
	{
		let card = randomCards();
		showCard(card, DEALER);
		updateScore(card, DEALER);
		showScore(DEALER);
		await sleep(1000);
	}

	blackjackGame['turnsOver'] = true;
	let winner = computeWinner();
	showResult(winner);
}
function computeWinner()
{
	let winner;
	if(YOU['score'] <= 21)
	{
		if(YOU['score'] > DEALER['score'] || DEALER['score'] > 21)
		{
			console.log('You Won!');
			winner = YOU;
			blackjackGame['wins']++;
		}
		else if(YOU['score'] < DEALER['score'])
		{
			console.log('You Lost!');
			winner = DEALER;
			blackjackGame['losses']++;
		}
		else if(YOU['score'] === DEALER['score'])
		{
			console.log('You Drew!');
			blackjackGame['draws']++;
		}
	}
	else if(YOU['score'] > 21 && DEALER['score'] <= 21)
	{
		console.log('You Lost!');
		winner = DEALER;
		blackjackGame['losses']++;
	}
	else if(YOU['score'] > 21 && DEALER['score'] > 21)
	{
		console.log('You Drew!');
		blackjackGame['draws']++;
	}
	console.log('Winner is : ', winner);
	console.log(blackjackGame);
	return winner;
}
function showResult(winner)
{
	let message, messageColor;
	if(blackjackGame['turnsOver'] === true)
	{
		if(winner === YOU)
		{
			message = 'You Won!';
			messageColor = "green";
			winSound.play();
			document.querySelector('#wins').textContent = blackjackGame['wins'];
		}
		else if(winner === DEALER)
		{
			message = 'You Lost!';
			messageColor = 'red';
			lostSound.play();
			document.querySelector('#losses').textContent = blackjackGame['losses'];
		}
		else
		{
			message = 'You Drew!';
			document.querySelector('#draws').textContent = blackjackGame['draws'];
		}
		document.querySelector('#blackjack-result').textContent = message;
		document.querySelector('#blackjack-result').style.color = messageColor;
	}
}