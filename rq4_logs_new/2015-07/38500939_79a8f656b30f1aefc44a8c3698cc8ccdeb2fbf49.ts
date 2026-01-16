class Game extends ex.Engine {

  constructor() {
    super({ width: 800, height: 600,canvasElementId: "game" });
  }

  public start() {
    var levelOne = new LevelOne(game);
    this.addScene('levelOne', levelOne);

    return super.start().then(() => {
      this.goToScene('levelOne');

    });
  }
}

var game = new Game();
game.start();