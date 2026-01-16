/* Treehouse FSJS Techdegree
 * Project 4 - OOP Game App
 * Game.js */

/** Class loading a game instance. */
class Game {
    /**
     * Set game parameters
     */
    constructor() {
        this.missed = 0;
        this.phrases = [
            new Phrase("The cake is a lie"),
            new Phrase("Our princess is in another castle"),
            new Phrase("Live long and prosper"),
            new Phrase("May the Force be with you"),
            new Phrase("You shall not pass")
        ];
        this.activePhrase;
    };
    
    /**
     * Start the game : choose a random phrase and display boxes for letters
     */    
    startGame() {
        document.getElementById("overlay").style.display = 'none';
        this.activePhrase = this.getRandomPhrase();
        this.activePhrase.addPhraseToDisplay();
    };

    /**
     * Return a random phrase
     * @return {object: Phrase} the choosen phrase
     */    
    getRandomPhrase() {
        const index = Math.floor(Math.random()*this.phrases.length);
        return this.phrases[index];
    };
    
    /**
     * action for the clicked key in the virtual keyboard or for the pressed a key in the real keyboard
     * @param {event} event - the event associated to the clicked button or to the pressed key
     */
    handleInteraction(event) {
        if (event.type == "click" && event.target.className == "key") {
            this.applyChoosenLetter(event.target)
        };
        if (event.type == "keyup" && /^[a-z]$/.test(event.key)) {
            const letter = event.key;
            const buttons = Array.from(document.querySelectorAll('div.keyrow button'));
            const activeButton = buttons.filter(button => button.textContent === letter)[0];
            if (activeButton.className == "key") { 
                this.applyChoosenLetter(activeButton)
            };
        }
    };
    
    /**
     * action to do after the player choose a letter
     * @param {event} event - the event associated to the pressed key
     */
    applyChoosenLetter(button) {
        const letter = button.textContent;
        if (this.activePhrase.checkLetter(letter)) {
            button.classList.add("chosen");
            this.activePhrase.showMatchedLetter(letter);
            if (this.checkForWin()) {
                this.gameOver(true);
            };
        } else {
            button.classList.add("wrong");
            this.removeLife();
        };
    };
    
    /**
     * Remove a life and check for losing
     */
    removeLife() {
        const divScoreBoard = document.getElementById("scoreboard");
        const listLI = divScoreBoard.firstElementChild.children;
        listLI[this.missed].firstElementChild.src = "images/lostHeart.png";
        this.missed += 1;
        if (this.missed == 5) {
            this.gameOver(false);
        };
    };
    
    /**
     * Check if the player found the phrase
     * @return {booleen} true if all letters are showed, false if at least one letter is hidded
     */
    checkForWin() {
        const divPhrase = document.getElementById("phrase");
        const listLI = Array.from(divPhrase.firstElementChild.children);
        return listLI.every(li => li.classList.contains("show") || li.classList.contains("space"))
    };
    
    /**
     * Display the overlay with a win or lose message
     * @param {booleen} win - true if the player found the phrase, false if not.
     */    
    gameOver(win) {
        const overlay = document.getElementById("overlay");
        const h = document.getElementById("game-over-message");
        if (win) {
            h.textContent = "Congratulations, you win! Can you do it again?";
            overlay.className = "win";
        } else {
            h.textContent = "Bad luck... Try to do better.";
            overlay.className = "lose";
        };
        overlay.style.display = '';
        this.resetGame();
    };
    
    /**
     * Set `missed` to 0 and show full hearts
     */
    resetLife() {
        const listLI = Array.from(document.querySelectorAll("li.tries"));
        listLI.forEach(li => {li.firstElementChild.src = "images/liveHeart.png"});
        this.missed = 0;
    };
    
    /**
     * Enable all the letters of the virtual keyboard
     */    
    resetKeyboard() {
        const listKey = Array.from(document.querySelectorAll("div.keyrow button"));
        listKey.forEach(key => {key.className="key"});
    };
    
    /**
     * Reset the game.
     */    
    resetGame() {
        this.activePhrase.resetLetter();
        this.resetKeyboard();
        this.resetLife();
    };
};