module Game {
    export class Controls {

        constructor() { }

        upPressed: boolean;
        downPressed: boolean;
        leftPressed: boolean;
        rightPressed: boolean;
        firePressed: boolean;

        HandleKeyboardEventUp = (e: KeyboardEvent) => {
            if (e.keyCode == 38) { // Player holding up
                this.upPressed = false;
            }
            if (e.keyCode == 40) { // Player holding down
                this.downPressed = false;
            }
            if (e.keyCode == 37) { // Player holding left
                this.leftPressed = false;
            }
            if (e.keyCode == 39) { // Player holding right
                this.rightPressed = false;
            }
            if (e.keyCode == 32) { //Fire key
                this.firePressed = false;
            }
        }

        HandleKeyboardEventDown = (e: KeyboardEvent) => {
            if (e.keyCode == 38) { // Player holding up
                this.upPressed = true;
            }
            if (e.keyCode == 40) { // Player holding down
                this.downPressed = true;
            }
            if (e.keyCode == 37) { // Player holding left
                this.leftPressed = true;
            }
            if (e.keyCode == 39) { // Player holding right
                this.rightPressed = true;
            }
            if (e.keyCode == 32) { //Fire key
                this.firePressed = true;
            }

        }
    }
} 