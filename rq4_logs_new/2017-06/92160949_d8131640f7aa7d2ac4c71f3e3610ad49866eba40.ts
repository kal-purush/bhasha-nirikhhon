/// <reference path="gameObject.ts" />


class Enemy extends GameObject implements Observer {

    private speedX: number;
    private speedY: number;

    private container: HTMLElement;
    private backgroundImage: String;


    constructor(container: HTMLElement, posX: number, posY: number, coinWidth: number, coinHeight: number, g:Game) {
        super("enemy", container, posX, posY, coinWidth, coinHeight, g);

        g.player.subscribe(this);

        let arrayList: string[] = ["trump", "putin", "merkel", "erdogan"];
        this.backgroundImage = arrayList[Math.floor(Math.random() * arrayList.length)];
        this.div.style.backgroundImage = "url(./images/" + this.backgroundImage + ".gif)";
        this.speedX = (Math.random() * 4);
        this.speedY = (Math.random() * 4);

        this.container = container;

    }

    public notify() {
        console.log("Player is Shrinking!!!!!");
        this.div.style.backgroundImage = "none";
        this.div.style.animation = "red-flashing 0.3s infinite";
        this.div.style.webkitAnimation = "red-flashing 0.3s infinite";
        this.div.style.animation = "red-flashing 0.25s infinite";

        setTimeout(() =>
            {
                this.div.style.animation = "none";
                this.div.style.backgroundImage = "url(./images/" + this.backgroundImage + ".gif)";
            },
        1700);

    }

    public move() {
        // console.log(speed);

        if( this.posY + 200 > this.container.clientHeight || this.posY < 0) { 
            this.speedY *= -1;
        }

        if( this.posX + 200 > this.container.clientWidth || this.posX < 0) { 
            this.speedX *= -1;
        }


        this.posX += this.speedX;
        this.posY += this.speedY;

        this.div.style.transform ="translate(" +this.posX + "px," + this.posY + "px)";

    }

}