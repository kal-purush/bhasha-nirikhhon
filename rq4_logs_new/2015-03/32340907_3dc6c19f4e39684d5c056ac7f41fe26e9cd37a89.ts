module states {

    var backButton: objects.Button;

    export function instructionState() {
    }

    //show the instruction screen
    export function showInstructions() {
        game = new createjs.Container();

        var instructionScreen = new createjs.Bitmap(assetLoader.getResult("instructions"));
        game.addChild(instructionScreen);

        stage.addChild(game);
        backButton = new objects.Button("button-sound", "start", 680, 400);
        game.addChild(backButton);
        backButton.addEventListener("click", backButtonClicked2);
        stage.addChild(game);


    }

    export function backButtonClicked2(event: MouseEvent) {
        createjs.Sound.play(backButton.soundString);
        stage.removeChild(game);
        game.removeAllChildren();
        game.removeAllEventListeners();
        currentState = constants.MENU_STATE;
        changeState(currentState);
    }

}//END module