const cards = document.querySelectorAll('.memory-card');

let hasFlippedCard = false;
let lockBoard = false;
let firstCard, secondCard;

function flipCard(){
    
    if(lockBoard) return;

    if(this === firstCard) return;
    
    this.classList.toggle('flip');

    if(!hasFlippedCard){
        hasFlippedCard = true;
        firstCard = this;

        return;
    }

    hasFlippedCard = false;
    secondCard = this;

    checkForMatch();
    
}

function checkForMatch(){
    let isMatch = firstCard.dataset.letter === secondCard.dataset.letter;
    isMatch ? disableFlip() : unflipCards();
    
}

function disableFlip(){
    firstCard.removeEventListener('click', flipCard);
    secondCard.removeEventListener('click', flipCard);

    resetBoard();
}

function unflipCards(){
    lockBoard = true;

    setTimeout(() => {
        firstCard.classList.remove('flip');
        secondCard.classList.remove('flip');

        lockBoard  = false;

        resetBoard();
    }, 1500);
}

function resetBoard(){
    [hasFlippedCard, lockBoard] = [false, false];
    [firstCard, secondCard] = [null, null];
}

(function shuffle(){
    cards.forEach(card => {
        let rndm = Math.floor(Math.random() * 12);
        card.style.order = rndm;
    });
})();

cards.forEach(card => card.addEventListener('click', flipCard));
