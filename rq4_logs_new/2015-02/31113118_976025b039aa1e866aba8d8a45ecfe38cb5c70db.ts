class Button {

    // Private Instance variable
    private _buttonImage: createjs.Bitmap;
    private _x: number;
    private _y: number;

    constructor(path: string, x: number, y: number) {
        this._buttonImage = new createjs.Bitmap(path);
        this.setX(x);
        this.setY(y);

        this._buttonImage.x = this._x;
        this._buttonImage.y = this._y;
        this._buttonImage.addEventListener("mouseover", this.mouseOver);
        this._buttonImage.addEventListener("mouseout", this.mouseOut);
    }

    public mouseOver(event: createjs.MouseEvent) {
        event.currentTarget.scaleX = 1.1; // 100% Alpha 
        event.currentTarget.scaleY = 1.1;
    }

    public mouseOut(event: createjs.MouseEvent) {
        event.currentTarget.scaleX = 1; // 100% Alpha 
        event.currentTarget.scaleY = 1;
    }

    public getImage(): createjs.Bitmap {
        return this._buttonImage;
    }
    public getX(): number {
        return this._x;
    }
    public getY(): number {
        return this._y
    }
    public setX(x: number) {
        this._x = x;
    }
    public setY(y: number) {
        this._y = y;
    }
} 