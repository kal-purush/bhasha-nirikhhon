module objects {
    export class Bullet extends objects.GameObject {

        //Constructor/////////////////////////////////////////////////////////////////////////////
        constructor() {
            super("bullet");
            this._dx = 25;
            this.soundString = "collect";
        } //constructor ends

        //Private Methods/////////////////////////////////////////////////////////////////////////

        //check to see if the object has reached the end of the screen or has hit the player
        private _checkBounds() {
            if (this.x < -480 + this.height || this.isColliding == true) {
                this.visible = false;
                this.isColliding = false;
            } //if ends
        } //method checkBounds ends

        //Public Methods//////////////////////////////////////////////////////////////////////////
        update() {
            this.x += this._dx;
            //  this._checkBounds();
        } //method update ends
    } //class Plane ends
} //module objects ends   