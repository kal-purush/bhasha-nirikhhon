/**Class representing the Game */
class Game{
    /** The canvas object the game is rendered on */
    private canvas: HTMLCanvasElement;

    /** The createjs stage object that is easier to work with */
    private stage: createjs.Stage;

    constructor(canvas:HTMLCanvasElement){
        this.canvas = canvas;
        this.stage = new createjs.Stage(canvas);
        let ball = new GameObject(100, 100, new createjs.SpriteSheet({
            images: ["http://www.gritengine.com/luaimg/circle.png"],
            frames: {
                width: 64,
                height: 64,
                count: 1
            }
        }));
        this.stage.addChild(ball.sprite);
        this.stage.update();
    }
}