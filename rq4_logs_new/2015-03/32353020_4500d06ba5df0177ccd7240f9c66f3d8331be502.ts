module objects {
    export class Laser extends objects.GameObject {


        //constructor ++++++++++++++++++++++++++++
        constructor() {
            super("laser");
            this._dx = 10;

            this.soundString = "laser_sound";


            //set island to start at random x
            this._reset();

        }
        //private methods++++++++++++++++++++++++++
        private _reset() {
            this.y = stage.mouseY;
        }
        private _checkbounds() {
            if (this.x <= 480 + this.width) {
                this._reset();

            }
        }



        //public methods+++++++++++++++++++++++++++
        public update() {
            this.x += this._dx;
            this._checkbounds();



        }
    }
}  