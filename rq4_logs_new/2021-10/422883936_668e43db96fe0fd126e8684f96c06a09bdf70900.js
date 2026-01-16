document.addEventListener('DOMContentLoaded', () => {
	// Card options
	const cardArray = [
		{
			name: 'fries',
			img: 'images/fries.png',
		},
		{
			name: 'fries',
			img: 'images/fries.png',
		},
		{
			name: 'Cheeseburger',
			img: 'images/Cheeseburger.png',
		},
		{
			name: 'Cheeseburger',
			img: 'images/Cheeseburger.png',
		},
		{
			name: 'hotdog',
			img: 'images/hotdog.png',
		},
		{
			name: 'hotdog',
			img: 'images/hotdog.png',
		},
		{
			name: 'ice-cream',
			img: 'images/ice-cream.png',
		},
		{
			name: 'ice-cream',
			img: 'images/ice-cream.png',
		},
		{
			name: 'milkshake',
			img: 'images/milkshake.png',
		},
		{
			name: 'milkshake',
			img: 'images/milkshake.png',
		},
		{
			name: 'pizza',
			img: 'images/pizza.png',
		},
		{
			name: 'pizza',
			img: 'images/pizza.png',
		},
	];

	// Randomize card array
	cardArray.sort(() => 0.5 - Math.random());

	const grid = document.querySelector('.grid');
	const resultDisplay = document.querySelector('#result');

	let cardsChosen = [];
	let cardsChosenId = [];
	let cardsWon = [];

	// Create your board
	function createBoard() {
		// Loop over cards
		for (let i = 0; i < cardArray.length; i++) {
			const card = document.createElement('img');

			// Add class name
			card.classList.add('card-img');

			// Link to the path of the img
			card.setAttribute('src', 'images/blank.png');

			// Set data id to img setAttribute
			card.setAttribute('data-id', i);

			card.addEventListener('click', flipCard);

			// Add the Card into the grid class
			grid.appendChild(card);
		}
	}

	// Check for match
	function checkForMatch() {
		const cards = document.querySelectorAll('img');
		const optionOneId = cardsChosenId[0];
		const optionTwoId = cardsChosenId[1];

		if (optionOneId == optionTwoId) {
			cards[optionOneId].setAttribute('src', 'images/blank.png');
			cards[optionOneId].setAttribute('src', 'images/blank.png');
			alert('You have clicked the same image!');
		} else if (cardsChosen[0] === cardsChosen[1]) {
			alert('You found a match');

			// sign the white cards
			cards[optionOneId].setAttribute('src', 'images/white.png');
			cards[optionTwoId].setAttribute('src', 'images/white.png');

			// remove from grid
			cards[optionOneId].removeEventListener('click', flipCard);
			cards[optionTwoId].removeEventListener('click', flipCard);

			// Add good cards into cards won
			cardsWon.push(cardsChosen);
		} else {
			// Flip cards back to play again
			cards[optionOneId].setAttribute('src', 'images/blank.png');
			cards[optionTwoId].setAttribute('src', 'images/blank.png');
			alert('Sorry try again');
		}

		// Stop flipping again
		cardsChosen = [];
		cardsChosenId = [];

		// finish alert when all cards are flipped
		resultDisplay.textContent = cardsWon.length;
		if (cardsWon.length === cardArray.length / 2) {
			resultDisplay.textContent = 'Congratulation! You have found them all';
		}
	}

	// Flip the cards
	function flipCard() {
		// Get data from data-id attribute
		const cardId = this.getAttribute('data-id');

		// Match card id full and locate the name of the card
		cardsChosen.push(cardArray[cardId].name);

		// Add id in card
		cardsChosenId.push(cardId);
		console.log(cardArray[cardId].name);

		// Add img to the square based on the card id it hold
		this.setAttribute('src', cardArray[cardId].img);

		// Check only 2 cards
		if (cardsChosen.length === 2) {
			setTimeout(checkForMatch, 500);
		}
	}

	// Reset game Not done yet
	function playAgain() {
		const playAgain = document.getElementById('play-again');

		playAgain.addEventListener('click', () => {
			// Reset images from array
			document
				.querySelectorAll('img')
				.forEach((elem) => elem.setAttribute('src', 'images/blank.png'));

			// Fix reset after winning the game ###

			// Add new array from random cards

			// display all resets card in page
		});
	}

	playAgain();
	createBoard();
});