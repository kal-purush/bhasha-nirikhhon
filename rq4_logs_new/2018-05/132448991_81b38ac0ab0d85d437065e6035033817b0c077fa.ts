class Game {
    // Game instance
    private static gameInstance: Game;

    // Get Instance of game or create one (SingleTon)
    public static getInstance() {
        if (!Game.gameInstance) {
            Game.gameInstance = new Game();
        }
        return Game.gameInstance;
    }

    private constructor() {
        // Container div for the game
        let container = document.getElementById("container");

        // GameLoop function
        requestAnimationFrame(() => this.gameLoop());
    }

    private gameLoop(): void {
        requestAnimationFrame(() => this.gameLoop());
    }
}

// load game
window.addEventListener("load", function () {
    let game = Game.getInstance();
});