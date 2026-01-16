//Porject Name: Assignment2-Slot Machine
//Programmer: Bhavin Patel
//Description: Button Object
//Date: Feb 20 2015
//Version 4.0 Add buton obejct

module objects {
export class Button {

        //Private Variables
        private _buttonImage: createjs.Bitmap;
        private _x: number;
        private _y: number;

        constructor(path: string, x: number, y: number) {
            this.setX(x);
            this.setY(y);
            this._buttonImage = new createjs.Bitmap(path);
            this._buttonImage.x = this._x;
            this._buttonImage.y = this._y;
            this._buttonImage.addEventListener("mouseover", function (event: createjs.MouseEvent) {
                event.currentTarget.alpha = 0.5
        })
        this._buttonImage.addEventListener("mouseout", this._buttonOut)
    }

        ///Getters and Setters for the X and Y Co-ordinates
        public getX(): number {
            return this._x;
        }
        public setX(x: number) {
            this._x = x;
        }

        public getY(): number {
            return this._y;
        }
        public setY(y: number) {
            this._y = y;
        }

        public getImage(): createjs.Bitmap {
            return this._buttonImage;
        }

        //Event Handlers
        private _buttonOver(event: createjs.MouseEvent): void {
            event.currentTarget.alpha = 1.0;
        }

        private _buttonOut(event: createjs.MouseEvent): void {
            event.currentTarget.alpha = 1.0;
        }


    } 
}