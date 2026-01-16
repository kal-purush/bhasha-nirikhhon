module states {
    export function menuState() {
        background.update();
    } //function menuState ends

    export function menu() {
        stage.removeAllChildren(); //clear the stage
        stage.removeAllEventListeners(); //remove event listeners

        background = new objects.Background();
        stage.addChild(background);

        //add the menu screen
        menuScreen = new createjs.Bitmap("assets/images/MenuScreen.png");
        menuScreen.x = 320;
        stage.addChild(menuScreen);

        //add the start button
        startButton = new createjs.Bitmap("assets/images/StartButton.png");
        startButton.x = 352;
        startButton.y = 398;
        stage.addChild(startButton);

        //add the instructions button
        instructionsButton = new createjs.Bitmap("assets/images/InstructionsButton.png");
        instructionsButton.x = 828;
        instructionsButton.y = 398;
        stage.addChild(instructionsButton);

        startButton.addEventListener("click", startButtonClick);
        startButton.addEventListener("mouseover", startButtonMouseOver);
        startButton.addEventListener("mouseout", startButtonMouseOut);
    } //function menu ends

    export function startButtonClick() {
        changeState(constants.PLAY_STATE);
    } //function playButtonClick ends

    export function startButtonMouseOver() {
        startButton.alpha = 0.01;
    } //function playButtonMouseOver ends

    export function startButtonMouseOut() {
        startButton.alpha = 1;
    } //function playButtonMouseOver ends

} //module states ends