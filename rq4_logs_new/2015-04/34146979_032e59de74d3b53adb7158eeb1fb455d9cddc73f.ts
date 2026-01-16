///<reference path="lib\jquery.d.ts" />
///<reference path="lib\lodash.d.ts" />

var $gameWindow;
var activeObjs = [];

interface IRect {
    left: number;
    top: number;
    width: number;
    height: number;
}

class gameObj {
    private static _nextID: number = 0;

    public _$elem;

    public ID: number;
    private _boundBox: IRect;

    constructor(data: any) {
        this._$elem = $('<div></div>');

        this._$elem.addClass(data.type);

        this._boundBox = {
            left: data.position[0],
            top: data.position[1],
            height: data.height,
            width: data.width
        };

        this._$elem.css(this._boundBox);

        this.ID = gameObj._nextID++;
    }

    public update(deltaTime: number) {}

    public onCollision(obj: gameObj):boolean {
        return true;
    }

    public getElement() {
        return this._$elem;
    }

    public position(position?: number[], checkCollision: boolean = true) {
        if(!!position) {
            var origPosition = [
                this._boundBox.left,
                this._boundBox.top
            ];

            this._boundBox.left = position[0];
            this._boundBox.top = position[1];
            this._$elem.css(this._boundBox);

            if(checkCollision) {
                if(!this.isInGameWindow()) {
                    this.position(origPosition, false);
                } else {
                    var xCol = _.any(activeObjs, (obj) => {

                    })
                }


            }
        }

        return [
            this._boundBox.left,
            this._boundBox.top
        ];
    }

    public checkCollisions(): boolean {
        if(!this.isInGameWindow()) {
            return false;
        }

        var result: boolean = true;

        _.each(activeObjs, (obj) => {
            if(this.isColliding(obj) && !obj.onCollision(this)) {
                result = false;
            }
        });

        return result;
    }

    public isInGameWindow(): boolean {
        return this._boundBox.left >= 0
            && this._boundBox.top >= 0
            && this._boundBox.left + this._boundBox.width <= $gameWindow.width()
            && this._boundBox.top + this._boundBox.height <= $gameWindow.height();
    }

    public isCollidingX(obj: gameObj):boolean {
        var thisRect = {
            top: this._boundBox.top,
            bottom: this._boundBox.top + this._boundBox.height,
            left: this._boundBox.left,
            right: this._boundBox.left + this._boundBox.width
        };
        var objRect = {
            top: obj._boundBox.top,
            bottom: obj._boundBox.top + obj._boundBox.height,
            left: obj._boundBox.left,
            right: obj._boundBox.left + obj._boundBox.width
        };

        return (thisRect.top > objRect.top && thisRect.top < objRect.bottom)
        || (thisRect.bottom < objRect.bottom && thisRect.bottom > objRect.top);
    }

    public isCollidingY(obj: gameObj): boolean {
        var thisRect = {
            top: this._boundBox.top,
            bottom: this._boundBox.top + this._boundBox.height,
            left: this._boundBox.left,
            right: this._boundBox.left + this._boundBox.width
        };
        var objRect = {
            top: obj._boundBox.top,
            bottom: obj._boundBox.top + obj._boundBox.height,
            left: obj._boundBox.left,
            right: obj._boundBox.left + obj._boundBox.width
        };

        return (thisRect.left > objRect.left && thisRect.left < objRect.right)
            || (thisRect.right < objRect.right && thisRect.right > objRect.left);
    }

    public isColliding(obj: gameObj):boolean {
        var thisRect = {
            top: this._boundBox.top,
            bottom: this._boundBox.top + this._boundBox.height,
            left: this._boundBox.left,
            right: this._boundBox.left + this._boundBox.width
        };
        var objRect = {
            top: obj._boundBox.top,
            bottom: obj._boundBox.top + obj._boundBox.height,
            left: obj._boundBox.left,
            right: obj._boundBox.left + obj._boundBox.width
        };

        var yCol =
        var xCol =

        return yCol && xCol;
    }

    public destroy() {}
}

class grass extends gameObj {
    constructor(data: any) {
        super(data);
    }

    public onCollision(obj: gameObj) {
        return false;
    }
}

class trigger extends grass {

    public target: number;

    private _$buttonElem;
    private _isActive;

    constructor(data: any) {
        super(data);

        this._$buttonElem = $('.target[target=' + this.target + ']');
        this._$buttonElem.enabled = true;
        this._$buttonElem.click(_.bind(this.onClick, this));

        this.target = data.target;
    }

    public active(isActive?: boolean) {
        if(typeof isActive !== 'undefined') {
            this._isActive = isActive;
        }

        return this._isActive;
    }

    private onClick() {
        this._isActive = true;
    }
}

class spawn extends gameObj {

    private timeout: number;
    private enemies: string[] = [];

    private timer: number;

    constructor(data: any) {
        super(data);

        this.enemies = data.enemies;
        this.timeout = data.timeout;
        this.timer = 0;
    }

    public update(deltaTime: number) {
        if(this.enemies.length == 0) {
            return;
        }

        this.timer += deltaTime;

        if(this.timer > this.timeout) {
            this.spawnNextEnemy();
            this.timer = 0;
        }
    }

    private spawnNextEnemy() {
        var pos = this.position();
        var enemy = gameObjFactory({
            type: this.enemies.pop(),
            position: pos
        });

        $gameWindow.append(enemy.getElement());
    }
}

class spike extends gameObj {
    constructor(data: any) {
        super(data);
    }

    public onCollision(obj: gameObj) {
        removeGameObj(obj);
        return true;
    }
}

class base extends gameObj {

    public health: number = 100;

    constructor(data: any) {
        super(data);
    }

    public onCollision(obj: gameObj) {
        this.health -= 10;
        removeGameObj(obj);
        return true;
    }
}

class enemy extends gameObj {
    public static Count: number = 0;

    public _speed: number = 10;
    public _gravity: number = 10;

    private _direction: number = 1;

    constructor(data: any) {
        super(data);

        this._$elem.css({
            'z-index': 10
        });

        enemy.Count++;
    }

    public update(deltaTime: number) {
        var pos = this.position();

        this.position([
            pos[0] + (this._speed * this._direction),
            pos[1] + (this._gravity)
        ]);
    }

    public onTransform() {}

    public destroy() {
        enemy.Count--;
    }
}

class grunt extends enemy {
    constructor(data: any) {
        super({
            type: 'grunt',
            height: 30,
            width: 30,
            position: data.position
        });
    }
}

class bomber extends enemy {
    constructor(data: any) {
        super({
            type: 'bomber',
            height: 30,
            width: 30,
            position: data.position
        });
    }
}

class berzerker extends enemy {
    constructor(data: any) {
        super({
            type: 'berzerker',
            height: 30,
            width: 30,
            position: data.position
        });
    }
}

function checkEndState() {
    var base = _.find(activeObjs, (activeObj) => {
        return typeof activeObj === 'base';
    });

    if(base.health <= 0) {
        levelLost();
    } else if(enemy.Count == 0) {
        levelWon();
    }
}

function removeGameObj(obj: gameObj) {
    obj.destroy();

    activeObjs = _.reject(activeObjs, (activeObj) => {
        return obj.ID == activeObj.ID;
    });

    checkEndState();
}

function onTransformButtonClick() {

}

function gameObjFactory(data: any, isActive: boolean = true) {
    var obj =  new window[data.type](data);

    if(isActive) {
        activeObjs.push(obj);
    }

    return obj;
}

function init() {
    $gameWindow = $('.game-window');
    $('.button').click(onTransformButtonClick);

    loadLevel('one');
}

function loadLevel(levelName) {
    $.get('scripts/levels/' + levelName + '.json')
        .done(buildLevel);
}

function buildLevel(levelData) {
    document.title = levelData.name;

    _.each($('.target'), ($target) => {
        $target.disabled = false;
    });

    _.each(levelData.map, function(data) {
        var obj = gameObjFactory(data);

        $gameWindow.append(obj.getElement());
    });

    update(0);
}

function update(deltaTime: number) {
    _.each(activeObjs, function(obj: gameObj) {
        obj.update(deltaTime);
    });

    setTimeout(() => {
        update(500);
    }, 500);
}

function levelLost() {

}

function levelWon() {

}

$(init);