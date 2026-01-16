module objects {
    export class Bullet extends objects.GameObject {
        constructor(x: number, y: number) {
            super("bullet");

            this.x = x;
            this.y = y;
        } //constructor ends

        public update(): void {
            this.x += 5;
            if (this.x > 640) {
                tank.bulletOnScreen = false;
                stage.removeChild(this);
                console.log("Bullet off screen!");
            } //if ends
        } //method update ends
    } //class Bullet ends
} //module objects ends 