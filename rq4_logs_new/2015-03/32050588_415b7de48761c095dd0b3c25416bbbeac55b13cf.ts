module objects {
    export class Explosion extends createjs.Bitmap {
        //instance variables
        private timeStart: number;
        private timeEnd: number;

        //Constructor/////////////////////////////////////////////////////////////////////////////
        constructor(x: number, y: number) {
            super(assetLoader.getResult("explosion"));
            this.regX = this.getBounds().width * 0.5;
            this.regY = this.getBounds().width * 0.5;

            this.x = x;
            this.y = y;

            //start the timer
            this.timeStart = Date.now();
            this.timeEnd = this.timeStart + 1000;
        } //constructor ends

        //Public Methods//////////////////////////////////////////////////////////////////////////
        public checkTime(): void {
            if (Date.now() > this.timeEnd) {
                //remove the explosion
                explosions.splice(explosions.indexOf(this), 1);
                stage.removeChild(this);
                numberOfExplosions--;
            } //if ends
        } //method checkTime ends
    } //class explosion ends
} //module objects ends