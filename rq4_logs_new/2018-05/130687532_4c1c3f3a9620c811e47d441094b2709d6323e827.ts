class Game {
    private static instance: Game
    public static container = document.getElementById("container");

    public static getInstance() {
        if (!this.instance) {
            this.instance = new Game();
        }
        return this.instance;
    }

    private constructor() {
        requestAnimationFrame(() => this.gameLoop());
    }

    private gameLoop() {
        requestAnimationFrame(() => this.gameLoop());
    }

    private gameOver(): void {
        new GameOver();
    }
}

window.addEventListener("load", function () {
    let game = Game.getInstance();
});