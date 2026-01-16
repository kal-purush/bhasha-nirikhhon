module objects {
    export class CarePackage extends objects.GameObject {
        //Constructor/////////////////////////////////////////////////////////////////////////////
        constructor() {
            super("carePackage");

            this._reset();
        } //constructor ends

        //Private Methods/////////////////////////////////////////////////////////////////////////
        private _reset() {
            this.x = 1280 + this.width;
            this.y = Math.floor(Math.random() * 480);
        } //method reset ends

        private _checkBounds() {
            if (this.x < 0 - this.width) {
                this._reset();
            } //if ends
        } //method checkBounds ends

        //Public Methods//////////////////////////////////////////////////////////////////////////
        public update(): void {
            this.x -= 5;

            this._checkBounds();
        } //method update ends

        public collide(): void {
            score += 300;
            this._reset();
        } //method collide ends
    } //class CarePackage ends
} //module objects ends  