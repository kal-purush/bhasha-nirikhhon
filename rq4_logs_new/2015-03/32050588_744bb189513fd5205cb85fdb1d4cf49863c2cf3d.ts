module objects {
    export class Bolt extends objects.GameObject {
        private _creator: objects.Alien;

        //Constructor/////////////////////////////////////////////////////////////////////////////
        constructor(x: number, y: number) {
            super("bolt");

            this.x = x;
            this.y = y;
        } //constructor ends

        //Public Methods//////////////////////////////////////////////////////////////////////////
        public update(): void {
            this.x -= 10;
            if (this.x < 0) {
                numberOfBolts--;
                bolts.splice(bolts.indexOf(this), 1);
                stage.removeChild(this);
            } //if ends
        } //method update ends

        public collide(): void {
            numberOfBolts--;
            bolts.splice(bolts.indexOf(this), 1);
            stage.removeChild(this);
        } //method collide ends
    } //class Bullet ends
} //module objects ends 