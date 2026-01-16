var canvas;
var stage: createjs.Stage;

// Game Objects 
var helloText: createjs.Text;
var button: createjs.Bitmap;

function init() {
    canvas = document.getElementById("canvas");
    stage = new createjs.Stage(canvas);
    stage.enableMouseOver(20); // Enable mouse events
    createjs.Ticker.setFPS(60); // 60 frames per second
    createjs.Ticker.addEventListener("tick", gameLoop);

    main();
}

function gameLoop() {
    helloText.rotation += 5;


    stage.update(); // Refreshes our stage
}

// Event handlers
function buttonClicked() {
    helloText.text = "Good Bye";
    helloText.regX = helloText.getBounds().width * 0.5;
    helloText.regY = helloText.getBounds().height * 0.5;
    
}

function buttonOut() {
    button.alpha = 1.0;
}

function buttonOver() {
    button.alpha = 0.5;
}





// Our Game Kicks off in here
function main() {
    // Bitmap Button
    button = new createjs.Bitmap("assets/images/blueButton.png");
    stage.addChild(button);
    button.x = stage.canvas.width * 0.5;
    button.y = stage.canvas.height * 0.5;
    button.regX = 110;
    button.regY = 110;

    button.addEventListener("click", buttonClicked);
    button.addEventListener("mouseover", buttonOver);
    button.addEventListener("mouseout", buttonOut);
   

    // Label
    helloText = new createjs.Text("Hello World!", "40px Consolas", "#000000");
    stage.addChild(helloText);
    helloText.x = stage.canvas.width * 0.5;
    helloText.y = stage.canvas.height * 0.5;
    helloText.regX = helloText.getBounds().width * 0.5;
    helloText.regY = helloText.getBounds().height * 0.5;

    
}