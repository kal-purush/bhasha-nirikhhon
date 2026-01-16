function init(){
    let canvas = <HTMLCanvasElement> document.getElementById('gameCanvas');
    canvas.style.background = '#000';
    let game = new Game(canvas );
}

class Game{
    private canvas: HTMLCanvasElement;
    private stage: createjs.Stage;

    constructor(canvas:HTMLCanvasElement){
        this.canvas = canvas;
        this.stage = new createjs.Stage(canvas);
        let circle = new createjs.Shape();
        circle.graphics.beginFill("DeepSkyBlue").drawCircle(0,0,50);
        circle.x = 100;
        circle.y = 100;
        console.log(circle);
        this.stage.addChild(circle);
        this.stage.update();
    }
}