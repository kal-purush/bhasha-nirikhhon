module t4 {
    var cat: Grix;

    var cats = ["catBlack", "catBlue", "catOrange", "catWhite"]
    var dir = ["_back", "_left", "_right", "_top"]
    var dirMap = [2, 0, 1, 3]

    var currCat = 0;
    var animation = 0;
    var direction = 1;

    var x = 200;
    var y = 200;
    var width: number;
    var height: number;

    export function setup() {
        var catSprite = Plena.loadSpriteFile("cats.png");
        for (var i = 0; i < 4; i++)loadCat(i, catSprite)

        cat = new Grix()
            .animationFromSprite(catSprite)
            .populate();

        Keyboard.addPressedEvent(top, Keyboard.KEY_W, Keyboard.KEY_UP)
        Keyboard.addPressedEvent(left, Keyboard.KEY_A, Keyboard.KEY_LEFT)
        Keyboard.addPressedEvent(right, Keyboard.KEY_D, Keyboard.KEY_RIGHT)
        Keyboard.addPressedEvent(back, Keyboard.KEY_S, Keyboard.KEY_DOWN)
        Keyboard.addPressedEvent(swich, Keyboard.KEY_SPACE)
    }

    export function update() {
        animation += 0.1;

        x += 2*Math.cos((Math.PI / 2) * direction);
        y += 2 * Math.sin((Math.PI / 2) * direction);

        if (x > 500) x = -width
        else if (x < -width)x = 500;
        if (y > 500) y = -height
        else if (y < -height) y = 500;
    }

    export function render() {
        for (var i = 0; i < 4; i++) {
            cat.moveTo(250 - cat.getWidth() * 2 + cat.getWidth() * i, 0);
            cat.setActiveImg(cats[i])
            cat.render();
        }

        cat.clean();
        cat.setPivotMove(0, 0)
        cat.scaleToSize(100, 100)
        cat.moveTo(x, y)
        cat.animationStep(Math.floor(animation))
        cat.setActiveAnimation(cats[currCat] + dir[dirMap[direction]])
        cat.render();

        width = cat.getWidth();
        height = cat.getHeight();
    }

    function loadCat(color: number, sprite: Sprite) {
        sprite.addImg(cats[color], color * 3 * 32, 6 * 32, 32, 32)
            .addAnimImgs(cats[color] + dir[0], color * 3 * 32, 4 * 32, 32, 32, 3)
            .addAnimImgs(cats[color] + dir[1], color * 3 * 32, 5 * 32, 32, 32, 3)
            .addAnimImgs(cats[color] + dir[2], color * 3 * 32, 6 * 32, 32, 32, 3)
            .addAnimImgs(cats[color] + dir[3], color * 3 * 32, 7 * 32, 32, 32, 3)
    } 

    function top() { direction = 3 }
    function left() { direction = 2 }
    function right() { direction = 0 }
    function back() { direction = 1 }
    function swich() { currCat = (currCat + 1)%4 }
}

Plena.init(t4.setup, t4.render, t4.update, 500, 500, [0.4, 0.8, 0.6, 1]);