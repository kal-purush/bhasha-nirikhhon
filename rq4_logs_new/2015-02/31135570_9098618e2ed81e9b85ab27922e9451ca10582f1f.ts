module objects {
export    class Button {
        //private variables
        private _image: createjs.Bitmap;
        private _x: number;
    private _y: number;

    //CONSTRUCTOR
        constructor(path: string, x: number, y: number) {
            this._x = x;
            this._y = y;
            this._image = new createjs.Bitmap(path);
            this._image.x = this._x;
            this._image.y = this._y;

            this._image.addEventListener("mouseover", this._buttonOver);
            this._image.addEventListener("mouseout", this._buttonOut);
        }

        public setX(x: number): void {
            this._x = x;
        }

        public getX(): number {
            return this._x;
        }

        public setY(y: number): void {
            this._y = y;
        }

        public getY(): number {
            return this._y;
        }

        public getImage(): createjs.Bitmap {
            return this._image;
        }

        private _buttonOut(event: createjs.MouseEvent): void {
            event.currentTarget.alpha = 1; // 100% Alpha 

        }

        private _buttonOver(event: createjs.MouseEvent): void {
            event.currentTarget.alpha = 0.7;

        }
    } 
}