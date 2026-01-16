module states {
    export function instructionsState() {
        background.update();
    } //function menuState ends

    export function instructions() {
        stage.removeAllChildren(); //clear the stage
        stage.removeAllEventListeners(); //remove event listeners

        background = new objects.Background();
        stage.addChild(background);

        //add the menu screen
        instructionsScreen = new createjs.Bitmap("assets/images/InstructionsScreen.png");
        instructionsScreen.x = 320;
        stage.addChild(instructionsScreen);
    } //function instructions ends
} //module states ends