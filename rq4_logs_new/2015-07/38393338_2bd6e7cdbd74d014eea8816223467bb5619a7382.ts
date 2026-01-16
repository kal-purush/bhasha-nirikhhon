class BaseCell {
    private _gameType: any;
    private _gameRect: Rect;
    private _dir: Vector;
    private _speed: number;
    private _image: any;

    get gameType(): any {
        return this._gameType;
    }
    set gameType(newGameType: any) {
        this._gameType = newGameType;
    }

    get gameRect(): Rect {
        return this._gameRect;
    }
    set gameRect(newGameRect: Rect) {
        this._gameRect = newGameRect;
    }

    get dir(): Vector {
        return this._dir;
    }
    set dir(newDir: Vector) {
        if (newDir) {
            this._dir = newDir;
        }
        else {
            this._dir = new Vector(0, 1);
        }
    }
    
    get speed(): number {
        return this._speed;
    }
    set speed(newSpeed: number) {
        if (newSpeed) {
            this._speed = newSpeed;
        }
        else {
            this._speed = 0;
        }
    }

    get image(): any {
        return this._image;
    }
    set image(newImage: any) {
        this._image = newImage;
    }

    constructor(data: any) {
        this.gameType = data.gameType;
        this.gameRect = new Rect(0, 0, data.width, data.height);
        this.gameRect.center = data.position;
        this.dir = data.dir;
        this.speed = data.speed;
        this.image = new createjs.Container();
        this.image.zIndex = 1;
    }

    update(t: number) {
        console.log(343434)
        this.move(t);
    }

    move(t: number) {
        this.gameRect.center = new Point(this.gameRect.center.x + this.speed * t * this.dir.x,
                                         this.gameRect.center.y + this.speed * t * this.dir.y);
    }
}

class EnergyCell extends BaseCell {
    constructor(data: any) { super(data); }
}