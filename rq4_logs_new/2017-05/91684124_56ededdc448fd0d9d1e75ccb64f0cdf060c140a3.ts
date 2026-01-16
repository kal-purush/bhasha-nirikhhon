class Dying implements Behaviour {
    char: Character;
    private jumpDirection: number = -3;
    private jumpHeight: number = 0;
    private direction: string = "up";

    constructor(c: Character) {
        this.char = c;

        this.char.div.className = "dying";
    }

    draw() {

        this.char.y += this.jumpDirection;

        if (this.direction == "up") {
            this.jumpHeight++;
            if (this.jumpHeight > 25) {
                this.direction = "down";
            }
        } else if (this.direction == "down") {
            this.jumpDirection = 3;
            if (this.char.y > 600) {
                this.char.behaviour = new Idle(this.char);
                Game.getInstance().gameOver();
            }
        }
    }

    onKeyDown(e: KeyboardEvent) {

    }
    onKeyUp(e: KeyboardEvent) {

    }
}