let appState = {
  computer : {name: 'Computer', hand: [], bank: 100000},
  suits : ['C', 'D', 'H', 'S'],
  ranks : [2, 3, 4, 5, 6, 7, 8, 9, 10, 'J', 'Q', 'K', 'A'],
  deck : [], //List of cards
  shuffled: false,
  playerCount : 1, //Can change for multiplayer, based on user input
  players: [], //Using an array to maintain players
  roundState: {
    playerTurn: true,
    computerTurn: false,
    currentBet: 0,
    playerCardTotal: 0,
    computerCardTotal: 0
  }

}

// Card class to define card object for appstate and game
class Card {
  constructor(rank, suit) {
    this.rank = rank;
    this.suit = suit;
  }
}

// Player class to define player object, also uselful if I want to implement multiplayer
class Player {
  constructor(id, name, hand, bank) {
    this.id = id;
    this.name = name;
    this.hand = hand;
    this.bank = bank;
  }
}

//This function will be used to start the game
function startGame() {
  createDeck(appState);
  createPlayer(appState)
  shuffleDeck(appState);
  deal(appState);
}

//wanted to create a seperate function to generate a new deck if game rules/ state applies 
function createDeck({suits, ranks, deck}) {
  for(let i = 0; i < suits.length; i++) {
    for(let j = 0; j < ranks.length; j++) {
      let card = new Card(ranks[j], suits[i])
      deck.push(card)
    }
  }
}

function createPlayer({playerCount, players}) {
  for(let i = 0; i < playerCount; i++) {
    let player = new Player(i, 'Jarec', [], 2500);
    players.push(player)
  }
}

// Fisher Yates shuffle to effectivly shuffle deck
function shuffleDeck({deck}) {
  if(deck) {
    if(deck.length < 1) {
      alert('Deck not made, making new one now');
      createDeck(appState);
    } else {
      let m = deck.length, t, i;
      while(m) {
        i = Math.floor(Math.random() * m--);
        t = deck[m];
        deck[m] = deck[i];
        deck[i] = t;
      }
      return appState = {
        ...appState,
        deck: deck,
        shuffled: true
      };
    }
  }
}

function deal({players, computer, roundState, shuffled, deck}) {
  if(!shuffled) {
    alert('Shuffling deck');
    shuffleDeck(appState);
  } else {
    let hand = 0;
    while(hand < 2) {
      for(player of players) {
        player.hand.push(deck.pop());
        computer.hand.push(deck.pop());
        hand++
      }
    }
  }
}

function Turn({roundState, computer, players}) {
  let {playerTurn} = roundState;
  let {computerTurn} = roundState;
  let player = players[0];
  let {playerCardTotal} = roundState;
  let {computerCardTotal} = roundState;
  function playerAction() {
    let decision = prompt(`Your total is ${playerCardTotal}, would you like to hit or stay?`);
    if(decision === 'hit') {
      player.hand.push(appState.deck.pop())
      checkTotal(player)
    } else if(decision === 'stay') {
      return (playerTurn = false, computerTurn = true)
    }
  }

  function checkTotal({hand}) {
    alert(JSON.stringify(hand))
    let sum = 0;
    let faceValue = 10;
    if(playerTurn) {
      for(let i = 0; i < hand; i++) {
        console.log(hand[i].rank)
        if(typeof hand[i].rank === 'string') {
          sum += faceValue
        }
        sum += hand[i]
      }
      appState.roundState.playerCardTotal = sum
    } else {
      for(let i = 0; i < player.hand; i++) {
        if(typeof hand[i] === 'string') {
          sum += faceValue
        }
        sum += hand[i]
      }
      appState.roundState.computerCardTotal = sum
    }
  }

  if(playerTurn) {
    while(playerTurn) {
      checkTotal(player)
      playerAction()
    }
  } else {
    alert('computer turn started')
  }

}

startGame()
Turn(appState)
console.log(appState)