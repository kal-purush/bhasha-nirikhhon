(function (root, factory) {
  if (typeof define === 'function' && define.amd) {
    define(factory);
  }
  else if (typeof exports === 'object') {
    module.exports = factory;
  }
  else {
    root.blackjack = factory();
  }
})(this, function () {

  // locally scoped Object
  let blackjack = {};
  let dealCards = {};
  let chooseOption = {};
  let player = document.querySelector('.js-player');
  let dealer = document.querySelector('.js-dealer');
  let shuffle = document.querySelector('.js-shuffle');
  let deal = document.querySelector('.js-deal');
  let hit = document.querySelector('.js-hit');
  let stick = document.querySelector('.js-stick');
  let winner = document.querySelector('.js-winner');
  let winnderText = winner.querySelector('.js-winner-text');

  /**
   *  Randomly shuffle 52 cards with their suit
   */
  let shuffleCards = function () {
    let cardPack = {};
    let cards = [];

    let cardSuits = [
      'Hearts',
      'Spades',
      'Clubs',
      'Diamonds'
    ];
    let cardNumbers = [
      'Ace',
      '2',
      '3',
      '4',
      '5',
      '6',
      '7',
      '8',
      '9',
      '10',
      'Jack',
      'Queen',
      'King'
    ];

    for (let suit of cardSuits) {
       for (let card of cardNumbers) {
         cardPack = {
           suit: suit,
           card: card,
           value: card,
           order: Math.floor(Math.random() * 5200) + 1
         };
         if (cardPack.value === 'Jack' || cardPack.value === 'Queen' || cardPack.value === 'King') {
           cardPack.value = '10';
         }
         else if (cardPack.value === 'Ace') {
           cardPack.value = '11';
         }
         cards.push(cardPack);
       }
    }

    cards = cards.sort(function(a,b) {
      return (a.order < b.order ? -1 : 1)
    });

    if (dealCards.deal) {
      deal.removeEventListener('click', dealCards.deal);
    }

    dealCards.deal = function () {
      deal.classList.add('is-disabled');
      dealPack(cards);
    }

    deal.addEventListener('click', dealCards.deal);
  };

  /**
   *  Deal 2 cards to player and dealer
   *  Hide 1 of the dealers cards and their total
   */

  let dealPack = function (cards) {

    let playerHand = [];
    let dealerHand = [];

    playerHand.push(cards[0]);
    playerHand.push(cards[1]);

    // Store player total in session storage
    if (!sessionStorage.getItem('playerTotal')) {
      let playerTotal = Number(playerHand[0].value) + Number(playerHand[1].value);
      sessionStorage.setItem('playerTotal', playerTotal);
    }

    let playerCards = `<div class="js-player-cards"> <div class="row js-player-group">
                        <div class="col-xs-4">Card:</div>
                        <div class="col-xs-8">${playerHand[0].card} of ${playerHand[0].suit}</div>
                        <div class="col-xs-4">Card:</div>
                        <div class="col-xs-8">${playerHand[1].card} of ${playerHand[1].suit}</div>
                      </div>
                      <div class="row">
                        <div class="col-xs-4">Total:</div>
                        <div class="col-xs-8 js-player-total">${sessionStorage.getItem('playerTotal')}</div>
                      </div> </div>`;


    dealerHand.push(cards[2]);
    dealerHand.push(cards[3]);

    // Store dealer total in session storage
    if (!sessionStorage.getItem('dealerTotal')) {
      let dealerTotal = Number(dealerHand[0].value) + Number(dealerHand[1].value);
      sessionStorage.setItem('dealerTotal', dealerTotal);
    }
    let dealerCards = `<div class="js-dealer-cards">
                        <div class="row js-dealer-group">
                          <div class="col-xs-4">Card:</div>
                          <div class="col-xs-8 js-masked is-masked">${dealerHand[0].card} of ${dealerHand[0].suit}</div>
                          <div class="col-xs-4">Card:</div>
                          <div class="col-xs-8">${dealerHand[1].card} of ${dealerHand[1].suit}</div>
                        </div>
                        <div class="row">
                          <div class="col-xs-4">Total:</div>
                          <div class="col-xs-8 js-dealer-total is-hidden"><span>${sessionStorage.getItem('dealerTotal')}</span></div>
                        </div> 
                      </div>`;

    if (cards.length === 52) {
      cards = cards.slice(-48);
    }

    player.insertAdjacentHTML('afterbegin', playerCards);
    dealer.insertAdjacentHTML('afterbegin', dealerCards);

    if (sessionStorage.getItem('playerTotal') === 21 && dealerTotal !== 21) {
      playerWins();
    }
    else {
      hit.classList.remove('is-disabled');
      stick.classList.remove('is-disabled');

      if (chooseOption.stick) {
        stick.removeEventListener('click', chooseOption.stick);
      }

      if (chooseOption.hit) {
        hit.removeEventListener('click', chooseOption.hit);
      }

      chooseOption.stick = function () {
        hit.classList.add('is-disabled');

        stickDeck(cards, dealerHand);
      };

      chooseOption.hit = function () {
        hitDeck(cards, playerHand, dealerHand);
      };

      stick.addEventListener('click', chooseOption.stick);
      hit.addEventListener('click', chooseOption.hit);
    }
  };

  let hitDeck = function (cards, playerHand) {
    if (sessionStorage.getItem('playerTotal') < 22) {
      playerHand.push(cards[0]);
      let playerTotal = Number(sessionStorage.getItem('playerTotal')) + Number(cards[0].value);
      sessionStorage.setItem('playerTotal', playerTotal);

      cards.shift();

      let addLine =  `<div class="col-xs-4">Card:</div><div class="col-xs-8">${cards[0].card} of ${cards[0].suit}</div>`;

      player.querySelector('.js-player-group').insertAdjacentHTML('beforeend', addLine);

      player.querySelector('.js-player-total').innerText = sessionStorage.getItem('playerTotal');

      if (sessionStorage.getItem('playerTotal') > 21) {
        playerLooses();
      }
    }
    else {
       if (sessionStorage.getItem('playerTotal') > 21) {
          playerLooses();
       }
    }

  };

  let revealResults = function () {
    winner.classList.remove('is-hidden');
    stick.classList.add('is-disabled');
    hit.classList.add('is-disabled');
    shuffle.classList.remove('is-disabled');
    dealer.querySelector('.js-masked').classList.remove('is-masked');
    dealer.querySelector('.js-dealer-total').classList.remove('is-hidden');
  };

  let playerLooses = function () {
    revealResults();
    document.querySelector('.js-player-total').classList.add('is-loss');
    document.querySelector('.js-dealer-total').classList.add('is-win');
    winnderText.innerText = 'Dealer wins';
  };

  let playerWins = function () {
    revealResults();
    document.querySelector('.js-player-total').classList.add('is-win');
    document.querySelector('.js-dealer-total').classList.add('is-loss');
    winnderText.innerText = 'Player wins';
  };

  let draw = function () {
    revealResults();
    winnderText.innerText = 'It\'s a draw';
  };

  let stickDeck = function (cards, dealerHand) {
    for (let card of cards) {
      if (sessionStorage.getItem('dealerTotal') < 18) {
          dealerHand.push(card);

          let dealerTotal = Number(sessionStorage.getItem('dealerTotal')) + Number(card.value);
          sessionStorage.setItem('dealerTotal', dealerTotal);

          let addLine =  `<div class="col-xs-4">Card:</div><div class="col-xs-8">${card.card} of ${card.suit}</div>`;

          dealer.querySelector('.js-dealer-group').insertAdjacentHTML('beforeend', addLine);

          dealer.querySelector('.js-dealer-total').innerText = sessionStorage.getItem('dealerTotal');
      }
      else {
        if (sessionStorage.getItem('dealerTotal') > sessionStorage.getItem('playerTotal')  && sessionStorage.getItem('dealerTotal') < 22) {
          playerLooses();
        }

        if (sessionStorage.getItem('dealerTotal') > 21) {
          playerWins();
        }

        if (sessionStorage.getItem('dealerTotal') === sessionStorage.getItem('playerTotal')) {
          draw();
        }

        return false;
      }
    }

  };

  blackjack.shuffleBtn = function () {

    shuffle.addEventListener('click', function () {
      sessionStorage.removeItem('playerTotal');
      sessionStorage.removeItem('dealerTotal');

      if (deal.classList.contains('is-disabled')) {
        deal.classList.remove('is-disabled');
      }
      shuffle.classList.add('is-disabled');
      winner.classList.add('is-hidden');

      let dealerCards = document.querySelector('.js-dealer');
      let playerCards = document.querySelector('.js-player');
      let dealerCardsResults = document.querySelector('.js-dealer-cards');
      let playerCardsResults = document.querySelector('.js-player-cards');

      if (dealerCardsResults || playerCardsResults) {
        dealerCards.removeChild(dealerCardsResults);
        playerCards.removeChild(playerCardsResults);
      }

      shuffleCards();
    });
  };

  return blackjack;

});

blackjack.shuffleBtn();